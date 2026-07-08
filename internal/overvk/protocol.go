package overvk

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type uploadCacheKey struct {
	token  string
	peerID int
}

type cachedUploadURL struct {
	url        string
	createdAt  time.Time
	generation uint64
}

var (
	uploadCacheMu    sync.Mutex
	uploadCache      = map[uploadCacheKey]cachedUploadURL{}
	uploadGeneration uint64
)

// To: server\n + Type: data\n + SessionID: <UUID-36>\n + MessageID: <7digits>\n + Part: XXX/XXX\n + \n\n
const headerOverhead = 110

func base64Len(n int) int {
	return (n + 2) / 3 * 4
}

func maxTextPayload(engine EngineConfig) int {
	limit := engine.VKMessageMaxLength
	if CipherEnabled() {
		// base64(nonce + encrypt(plaintext) + tag) ≤ limit
		// → nonce + plaintext + tag ≤ floor(limit / 4) * 3
		maxCipher := (limit / 4) * 3
		limit = maxCipher - 28 // 12 nonce + 16 GCM tag
	}
	// headers + "\n\n" + base64(payload) ≤ limit
	maxBase64 := limit - headerOverhead
	// base64Len(n) = ceil(n/3)*4, so n ≤ floor(maxBase64/4)*3
	return (maxBase64 / 4) * 3
}

func UploadAndSendChunk(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, rotator *ChatRotator, to Target, sessionID string, sequence int, data []byte) error {
	metric := GetSessionMetrics(sessionID)
	started := time.Now()
	engine := Engine()
	isText := len(data) <= engine.TextMessageThreshold

	maxPayload := maxTextPayload(engine)

	if isText {
		numParts := (len(data) + maxPayload - 1) / maxPayload
		if numParts == 0 {
			numParts = 1
		}
		log.Printf("[%s] Sending %d bytes as %d TEXT message(s) (seq %d)", sessionID, len(data), numParts, sequence)
		for partIndex := 0; partIndex < numParts; partIndex++ {
			start := partIndex * maxPayload
			end := start + maxPayload
			if end > len(data) {
				end = len(data)
			}
			if err := sendAsTextMessage(ctx, client, accessToken, chatPeerID, to, sessionID, sequence, data[start:end], partIndex, numParts); err != nil {
				metric.RecordSend(len(data), true, numParts, false)
				return err
			}
		}
		metric.RecordSend(len(data), true, numParts, true)
		metric.RecordLatency(time.Since(started))
		return nil
	}

	log.Printf("[%s] Sending %d bytes as DOCUMENT (seq %d)", sessionID, len(data), sequence)
	if err := sendAsDocument(ctx, client, accessToken, chatPeerID, rotator, to, sessionID, sequence, data); err != nil {
		const maxTextFallbackParts = 16
		numParts := (len(data) + maxPayload - 1) / maxPayload
		if numParts > maxTextFallbackParts {
			log.Printf("[%s] Document upload failed for seq %d and TEXT fallback too large (%d parts): %v", sessionID, sequence, numParts, err)
			metric.RecordSend(len(data), false, 1, false)
			return err
		}
		fallbackPeerID := rotator.Next()
		log.Printf("[%s] Document upload failed for seq %d, falling back to %d TEXT messages via peer %d: %v", sessionID, sequence, numParts, fallbackPeerID, err)
		for partIndex := 0; partIndex < numParts; partIndex++ {
			start := partIndex * maxPayload
			end := start + maxPayload
			if end > len(data) {
				end = len(data)
			}
			if err := sendAsTextMessage(ctx, client, accessToken, fallbackPeerID, to, sessionID, sequence, data[start:end], partIndex, numParts); err != nil {
				metric.RecordSend(len(data), true, numParts, false)
				return err
			}
		}
		metric.RecordSend(len(data), true, numParts, true)
		metric.RecordLatency(time.Since(started))
		return nil
	}
	metric.RecordSend(len(data), false, 1, true)
	metric.RecordLatency(time.Since(started))
	return nil
}

func SendControlMessage(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, to Target, msgType MessageType, sessionID string, sequence int, payload string) error {
	message, err := EncryptMessageText(buildMessage(to, msgType, sessionID, sequence, payload, ""))
	if err != nil {
		return fmt.Errorf("encrypt control message %s for session %s: %w", msgType, sessionID, err)
	}
	_, err = APICall(ctx, client, "messages.send", url.Values{
		"peer_id":   {strconv.Itoa(chatPeerID)},
		"message":   {message},
		"random_id": {randomID()},
	}, accessToken)
	if err != nil {
		return fmt.Errorf("send control message %s for session %s: %w", msgType, sessionID, err)
	}
	return nil
}

func sendAsTextMessage(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, to Target, sessionID string, sequence int, data []byte, partIndex, totalParts int) error {
	partHeader := ""
	if totalParts > 1 {
		partHeader = fmt.Sprintf("Part: %d/%d\n", partIndex, totalParts)
	}
	payload := base64.StdEncoding.EncodeToString(data)
	message, err := EncryptMessageText(buildMessage(to, MessageData, sessionID, sequence, payload, partHeader))
	if err != nil {
		return fmt.Errorf("[%s] encrypt text message seq=%d part=%d/%d: %w", sessionID, sequence, partIndex, totalParts, err)
	}

	_, err = APICall(ctx, client, "messages.send", url.Values{
		"peer_id":   {strconv.Itoa(chatPeerID)},
		"message":   {message},
		"random_id": {randomID()},
	}, accessToken)
	if err != nil {
		return fmt.Errorf("[%s] send text message seq=%d part=%d/%d: %w", sessionID, sequence, partIndex, totalParts, err)
	}
	return nil
}

func sendAsDocument(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, rotator *ChatRotator, to Target, sessionID string, sequence int, data []byte) error {
	const maxUploadRetries = 5
	var lastErr error
	peerID := chatPeerID

	encData, err := Encrypt(data)
	if err != nil {
		return fmt.Errorf("[%s] encrypt document data: %w", sessionID, err)
	}

	for attempt := 0; attempt < maxUploadRetries; attempt++ {
		if attempt > 0 {
			peerID = rotator.Next()
			log.Printf("[%s] Retrying document upload seq=%d via peer %d (attempt %d/%d)", sessionID, sequence, peerID, attempt+1, maxUploadRetries)
		}

		uploadURL, _, err := getUploadURL(ctx, client, accessToken, peerID, sessionID)
		if err != nil {
			lastErr = err
			continue
		}

		uploadResult, err := uploadDocument(ctx, client, uploadURL, encData)
		if err != nil {
			invalidateUploadURL(accessToken, peerID, 0)
			lastErr = err
			continue
		}

		fileValue := stringFromAny(uploadResult["file"])
		if fileValue == "" {
			invalidateUploadURL(accessToken, peerID, 0)
			lastErr = fmt.Errorf("[%s] VK upload response did not contain file field", sessionID)
			continue
		}

		saveResp, err := APICall(ctx, client, "docs.save", url.Values{"file": {fileValue}}, accessToken)
		if err != nil {
			lastErr = fmt.Errorf("[%s] docs.save failed: %w", sessionID, err)
			continue
		}
		attachment, err := documentAttachmentString(saveResp)
		if err != nil {
			return fmt.Errorf("[%s] parse docs.save response: %w", sessionID, err)
		}

		headerBlock, err := EncryptMessageText(buildHeaderBlock(to, MessageData, sessionID, sequence, ""))
		if err != nil {
			return fmt.Errorf("[%s] encrypt document header: %w", sessionID, err)
		}
		_, err = APICall(ctx, client, "messages.send", url.Values{
			"peer_id":    {strconv.Itoa(peerID)},
			"message":    {headerBlock},
			"attachment": {attachment},
			"random_id":  {randomID()},
		}, accessToken)
		if err != nil {
			return fmt.Errorf("[%s] send document message seq=%d: %w", sessionID, sequence, err)
		}
		return nil
	}

	return lastErr
}

func getUploadURL(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, sessionID string) (string, uint64, error) {
	key := uploadCacheKey{token: accessToken, peerID: chatPeerID}
	now := time.Now()

	uploadCacheMu.Lock()
	if cached, ok := uploadCache[key]; ok && now.Sub(cached.createdAt) < Engine().UploadURLCacheTTL {
		uploadCacheMu.Unlock()
		return cached.url, cached.generation, nil
	}
	uploadCacheMu.Unlock()

	resp, err := APICall(ctx, client, "docs.getMessagesUploadServer", url.Values{
		"peer_id": {strconv.Itoa(chatPeerID)},
	}, accessToken)
	if err != nil {
		return "", 0, fmt.Errorf("[%s] docs.getMessagesUploadServer failed: %w", sessionID, err)
	}
	response, ok := resp["response"].(map[string]any)
	if !ok {
		return "", 0, fmt.Errorf("[%s] docs.getMessagesUploadServer missing response object", sessionID)
	}
	uploadURL := stringFromAny(response["upload_url"])
	if uploadURL == "" {
		return "", 0, fmt.Errorf("[%s] docs.getMessagesUploadServer missing upload_url", sessionID)
	}

	gen := atomic.AddUint64(&uploadGeneration, 1)
	uploadCacheMu.Lock()
	uploadCache[key] = cachedUploadURL{url: uploadURL, createdAt: now, generation: gen}
	uploadCacheMu.Unlock()
	return uploadURL, gen, nil
}

func invalidateUploadURL(accessToken string, chatPeerID int, generation uint64) {
	uploadCacheMu.Lock()
	defer uploadCacheMu.Unlock()
	key := uploadCacheKey{token: accessToken, peerID: chatPeerID}
	if generation == 0 {
		delete(uploadCache, key)
		return
	}
	if cached, ok := uploadCache[key]; ok && cached.generation == generation {
		delete(uploadCache, key)
	}
}

func uploadDocument(ctx context.Context, client *http.Client, uploadURL string, data []byte) (map[string]any, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	filePart, err := writer.CreateFormFile("file", "chunk.dat")
	if err != nil {
		return nil, err
	}
	if _, err := filePart.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("file upload failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	var decoded map[string]any
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func documentAttachmentString(saveResp map[string]any) (string, error) {
	response, ok := saveResp["response"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("missing response object")
	}

	doc, ok := response["doc"].(map[string]any)
	if !ok {
		if docs, ok := response["docs"].([]any); ok && len(docs) > 0 {
			doc, _ = docs[0].(map[string]any)
		}
	}
	if doc == nil {
		return "", fmt.Errorf("missing doc object")
	}

	ownerID := stringFromAny(doc["owner_id"])
	docID := stringFromAny(doc["id"])
	if ownerID == "" || docID == "" {
		return "", fmt.Errorf("missing owner_id or id in doc object")
	}
	return "doc" + ownerID + "_" + docID, nil
}

func ParseMessage(message vkMessage) ParsedMessage {
	messageText := message.Text
	if decrypted, err := DecryptMessageText(messageText); err == nil {
		messageText = decrypted
	}
	headerBlock := messageText
	payload := ""
	if before, after, ok := strings.Cut(messageText, "\n\n"); ok {
		headerBlock = before
		payload = strings.TrimSpace(after)
	}

	headers := map[string]string{}
	for _, line := range strings.Split(headerBlock, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		headers[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	var attachment *DocumentAttachment
	for _, item := range message.Attachments {
		if item.Type == "doc" && item.Doc != nil && item.Doc.URL != "" {
			attachment = &DocumentAttachment{URL: item.Doc.URL}
			break
		}
	}

	return ParsedMessage{PeerID: message.PeerID, Headers: headers, Payload: payload, Attachment: attachment}
}

func ProcessDataMessage(ctx context.Context, client *http.Client, message ParsedMessage, store *MultipartStore, incoming chan<- Packet, verbose bool) error {
	sessionID := message.Headers["SessionID"]
	if sessionID == "" {
		return nil
	}

	data, err := extractDataPayload(ctx, client, message)
	if err != nil {
		return err
	}
	GetSessionMetrics(sessionID).RecordReceive(len(data), true)

	sequence, err := strconv.Atoi(message.Headers["MessageID"])
	if err != nil {
		return fmt.Errorf("[%s] invalid MessageID %q: %w", sessionID, message.Headers["MessageID"], err)
	}

	if partHeader := message.Headers["Part"]; partHeader != "" {
		partIndex, totalParts, err := parsePartHeader(partHeader)
		if err != nil {
			return fmt.Errorf("[%s] invalid Part header %q: %w", sessionID, partHeader, err)
		}
		complete, ready, err := store.Add(sessionID, sequence, partIndex, totalParts, data)
		if err != nil {
			return err
		}
		if !ready {
			if verbose {
				log.Printf("[%s] Buffered part %d/%d for seq %d", sessionID, partIndex, totalParts, sequence)
			}
			return nil
		}
		data = complete
		if verbose {
			log.Printf("[%s] Reassembled %d parts for seq %d, total %d bytes", sessionID, totalParts, sequence, len(data))
		}
	}

	select {
	case incoming <- Packet{Sequence: sequence, Data: data}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func extractDataPayload(ctx context.Context, client *http.Client, message ParsedMessage) ([]byte, error) {
	sessionID := message.Headers["SessionID"]

	if message.Attachment != nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, message.Attachment.URL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("[%s] download VK document: %w", sessionID, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("[%s] document download returned HTTP %d", sessionID, resp.StatusCode)
		}
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return Decrypt(raw)
	}

	if message.Payload == "" {
		return nil, fmt.Errorf("[%s] DATA message without payload or attachment", sessionID)
	}
	data, err := base64.StdEncoding.DecodeString(message.Payload)
	if err != nil {
		return nil, fmt.Errorf("[%s] decode Base64 payload: %w", sessionID, err)
	}
	return data, nil
}

func parsePartHeader(value string) (int, int, error) {
	part, total, ok := strings.Cut(value, "/")
	if !ok {
		return 0, 0, fmt.Errorf("expected part/total")
	}
	partIndex, err := strconv.Atoi(strings.TrimSpace(part))
	if err != nil {
		return 0, 0, err
	}
	totalParts, err := strconv.Atoi(strings.TrimSpace(total))
	if err != nil {
		return 0, 0, err
	}
	return partIndex, totalParts, nil
}

func buildMessage(to Target, msgType MessageType, sessionID string, sequence int, payload string, extraHeaders string) string {
	headerBlock := buildHeaderBlock(to, msgType, sessionID, sequence, extraHeaders)
	return headerBlock + "\n\n" + payload
}

func buildHeaderBlock(to Target, msgType MessageType, sessionID string, sequence int, extraHeaders string) string {
	var builder strings.Builder
	builder.WriteString("To: ")
	builder.WriteString(string(to))
	builder.WriteByte('\n')
	builder.WriteString("Type: ")
	builder.WriteString(string(msgType))
	builder.WriteByte('\n')
	builder.WriteString("SessionID: ")
	builder.WriteString(sessionID)
	builder.WriteByte('\n')
	builder.WriteString("MessageID: ")
	builder.WriteString(strconv.Itoa(sequence))
	builder.WriteByte('\n')
	if extraHeaders != "" {
		builder.WriteString(extraHeaders)
	}
	return strings.TrimRight(builder.String(), "\n")
}

func randomID() string {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano()%2147483647, 10)
	}
	value := int64(buf[0])<<24 | int64(buf[1])<<16 | int64(buf[2])<<8 | int64(buf[3])
	if value < 0 {
		value = -value
	}
	return strconv.FormatInt(value%2147483647, 10)
}
