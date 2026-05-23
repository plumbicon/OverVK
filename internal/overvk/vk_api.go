package overvk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type VKAPIError struct {
	Method string
	Info   map[string]any
	Text   string
}

func (e *VKAPIError) Error() string {
	if e.Text != "" {
		return e.Text
	}
	if e.Info != nil {
		return fmt.Sprintf("vk api error in %s: %v", e.Method, e.Info)
	}
	return fmt.Sprintf("vk api error in %s", e.Method)
}

func NewHTTPClient() *http.Client {
	return &http.Client{Timeout: Engine().HTTPTimeout}
}

func APICall(ctx context.Context, client *http.Client, method string, params url.Values, accessToken string) (map[string]any, error) {
	return apiCall(ctx, client, method, params, accessToken, true)
}

func apiCall(ctx context.Context, client *http.Client, method string, params url.Values, accessToken string, logVKErrors bool) (map[string]any, error) {
	finalParams := url.Values{}
	for key, values := range params {
		for _, value := range values {
			finalParams.Add(key, value)
		}
	}
	finalParams.Set("v", Engine().APIVersion)
	finalParams.Set("access_token", accessToken)

	encoded := finalParams.Encode()
	endpoint := "https://api.vk.com/method/" + method
	const maxRetries = 5

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(encoded))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt < maxRetries-1 {
				delay := time.Duration(2*(attempt+1)) * time.Second
				log.Printf("network error calling VK method %q: %v; retrying in %s", method, err, delay)
				if sleepContext(ctx, delay) != nil {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, &VKAPIError{Method: method, Text: fmt.Sprintf("failed to call %s after %d retries: %v", method, maxRetries, err)}
		}

		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if attempt < maxRetries-1 {
				delay := time.Duration(2*(attempt+1)) * time.Second
				log.Printf("VK method %q returned HTTP %d; retrying in %s", method, resp.StatusCode, delay)
				if sleepContext(ctx, delay) != nil {
					return nil, ctx.Err()
				}
				continue
			}
			return nil, &VKAPIError{Method: method, Text: fmt.Sprintf("VK method %s returned HTTP %d: %s", method, resp.StatusCode, string(body))}
		}

		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			return nil, err
		}

		if rawErr, ok := decoded["error"].(map[string]any); ok {
			if intFromAny(rawErr["error_code"]) == 9 {
				log.Printf("VK flood control on %q; retrying in 5s", method)
				if sleepContext(ctx, 5*time.Second) != nil {
					return nil, ctx.Err()
				}
				continue
			}
			if logVKErrors {
				log.Printf("VK API error in %q: %v", method, rawErr)
			}
			return nil, &VKAPIError{Method: method, Info: rawErr}
		}

		return decoded, nil
	}

	return nil, &VKAPIError{Method: method, Text: fmt.Sprintf("failed to call %s after retries", method)}
}

type MessageHandler func(ctx context.Context, message ParsedMessage)

const chatPeerStartID = 2000000000
const bootstrapSessionID = "bootstrap"

func StartLongPollListener(ctx context.Context, client *http.Client, accessToken string, groupID int, peerIDs []int, handler MessageHandler) error {
	peerSet := make(map[int]struct{}, len(peerIDs))
	for _, peerID := range peerIDs {
		peerSet[peerID] = struct{}{}
	}

	log.Printf("starting VK Group Long Poll listener for group_id=%d", groupID)
	if len(peerIDs) > 0 {
		log.Printf("listening to peer IDs: %v", peerIDs)
	}

	server, key, ts, err := getLongPollDetails(ctx, client, accessToken, groupID)
	if err != nil {
		return err
	}
	log.Print("VK Group Long Poll server details obtained")

	for {
		if ctx.Err() != nil {
			return nil
		}

		checkURL, err := buildLongPollURL(server, key, ts)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
		if err != nil {
			return err
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("network error in Long Poll loop: %v; retrying", err)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil
			}
			continue
		}

		var lp longPollResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&lp)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			log.Printf("failed to decode Long Poll response: %v", decodeErr)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil
			}
			continue
		}
		if closeErr != nil {
			return closeErr
		}

		if lp.Failed != nil {
			log.Printf("Long Poll returned failed=%v; refreshing server details", lp.Failed)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil
			}
			server, key, ts, err = getLongPollDetails(ctx, client, accessToken, groupID)
			if err != nil {
				log.Printf("failed to refresh Long Poll details: %v", err)
				if sleepContext(ctx, Engine().LongPollDelay) != nil {
					return nil
				}
			}
			continue
		}

		if tsValue := stringFromAny(lp.TS); tsValue != "" {
			ts = tsValue
		}

		for _, update := range lp.Updates {
			if update.Type != "message_new" {
				log.Printf("bootstrap saw VK update type=%q", update.Type)
				continue
			}
			message := update.Object.VKMessage()
			if len(peerSet) > 0 {
				if _, ok := peerSet[message.PeerID]; !ok {
					continue
				}
			}
			parsed := ParseMessage(message)
			go handler(ctx, parsed)
		}
	}
}

func ListChatPeerIDs(ctx context.Context, client *http.Client, accessToken string, groupID int) ([]int, error) {
	const pageSize = 200
	var peerIDs []int
	seen := map[int]struct{}{}

	for offset := 0; ; offset += pageSize {
		resp, err := APICall(ctx, client, "messages.getConversations", url.Values{
			"offset":   {strconv.Itoa(offset)},
			"count":    {strconv.Itoa(pageSize)},
			"filter":   {"all"},
			"group_id": {strconv.Itoa(groupID)},
		}, accessToken)
		if err != nil {
			return nil, err
		}

		response, ok := resp["response"].(map[string]any)
		if !ok {
			return nil, errors.New("messages.getConversations response is missing response object")
		}
		items, ok := response["items"].([]any)
		if !ok {
			return nil, errors.New("messages.getConversations response is missing items array")
		}

		for _, item := range items {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			conversation, ok := itemMap["conversation"].(map[string]any)
			if !ok {
				continue
			}
			peer, ok := conversation["peer"].(map[string]any)
			if !ok {
				continue
			}
			peerID := intFromAny(peer["id"])
			if peerID < chatPeerStartID {
				continue
			}
			if _, ok := seen[peerID]; ok {
				continue
			}
			seen[peerID] = struct{}{}
			peerIDs = append(peerIDs, peerID)
		}

		total := intFromAny(response["count"])
		if len(items) < pageSize || offset+pageSize >= total {
			break
		}
	}

	scannedPeerIDs, err := ScanChatPeerIDs(ctx, client, accessToken, groupID, Engine().BootstrapPeerScanLimit)
	if err != nil {
		log.Printf("failed to scan VK chat peer IDs: %v", err)
	}
	for _, peerID := range scannedPeerIDs {
		if _, ok := seen[peerID]; ok {
			continue
		}
		seen[peerID] = struct{}{}
		peerIDs = append(peerIDs, peerID)
	}

	return peerIDs, nil
}

func ScanChatPeerIDs(ctx context.Context, client *http.Client, accessToken string, groupID int, limit int) ([]int, error) {
	if limit <= 0 {
		return nil, nil
	}

	peerIDs := make([]int, 0)
	for start := 1; start <= limit; start += 25 {
		if ctx.Err() != nil {
			return peerIDs, ctx.Err()
		}

		end := start + 24
		if end > limit {
			end = limit
		}
		batchPeerIDs, err := scanChatPeerIDBatch(ctx, client, accessToken, groupID, start, end)
		if err != nil {
			log.Printf("failed to scan VK chat peer IDs %d..%d via execute: %v", chatPeerStartID+start, chatPeerStartID+end, err)
			continue
		}
		peerIDs = append(peerIDs, batchPeerIDs...)
	}
	if len(peerIDs) > 0 {
		log.Printf("found VK chat peer IDs by scan: %v", peerIDs)
	}
	return peerIDs, nil
}

func scanChatPeerIDBatch(ctx context.Context, client *http.Client, accessToken string, groupID int, startChatID int, endChatID int) ([]int, error) {
	var code strings.Builder
	code.WriteString("var found=[];")
	for chatID := startChatID; chatID <= endChatID; chatID++ {
		peerID := chatPeerStartID + chatID
		code.WriteString("var r=API.messages.getConversationMembers({\"peer_id\":")
		code.WriteString(strconv.Itoa(peerID))
		code.WriteString(",\"group_id\":")
		code.WriteString(strconv.Itoa(groupID))
		code.WriteString("});")
		code.WriteString("if (r.count != null) { found.push(")
		code.WriteString(strconv.Itoa(peerID))
		code.WriteString("); }")
	}
	code.WriteString("return found;")

	resp, err := APICall(ctx, client, "execute", url.Values{
		"code": {code.String()},
	}, accessToken)
	if err != nil {
		return nil, err
	}

	rawResponse, ok := resp["response"].([]any)
	if !ok {
		return nil, errors.New("execute response is missing response array")
	}
	peerIDs := make([]int, 0, len(rawResponse))
	for _, item := range rawResponse {
		peerID := intFromAny(item)
		if peerID >= chatPeerStartID {
			peerIDs = append(peerIDs, peerID)
		}
	}
	return peerIDs, nil
}

func WaitForServerReady(ctx context.Context, client *http.Client, accessToken string, groupID int, phrase string) ([]int, error) {
	phrase = strings.TrimSpace(phrase)
	if phrase == "" {
		return nil, errors.New("handshake phrase must not be empty")
	}

	log.Printf("discovering shared VK chat peer IDs by announcing readiness for group_id=%d", groupID)

	server, key, ts, err := getLongPollDetails(ctx, client, accessToken, groupID)
	if err != nil {
		return nil, err
	}
	announced := map[int]struct{}{}
	readySet := map[int]struct{}{}
	var readyPeerIDs []int
	var collectDeadline time.Time

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if bootstrapCollectionDone(readyPeerIDs, collectDeadline) {
			log.Printf("server acknowledged shared peer IDs=%v", readyPeerIDs)
			return readyPeerIDs, nil
		}

		peerIDs, err := ListChatPeerIDs(ctx, client, accessToken, groupID)
		if err != nil {
			log.Printf("failed to list VK chat conversations: %v", err)
		}
		if len(peerIDs) == 0 {
			log.Print("no VK chat conversations found for client community")
		}
		for _, peerID := range peerIDs {
			announceClientReady(ctx, client, accessToken, peerID, phrase, announced)
		}

		checkURL, err := buildLongPollURLWithWait(server, key, ts, bootstrapLongPollWait(collectDeadline))
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			log.Printf("network error while waiting for server readiness: %v; retrying", err)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			continue
		}

		var lp longPollResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&lp)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			log.Printf("failed to decode server readiness Long Poll response: %v", decodeErr)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if closeErr != nil {
			return nil, closeErr
		}

		if lp.Failed != nil {
			log.Printf("Long Poll returned failed=%v while waiting for server readiness; refreshing server details", lp.Failed)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			server, key, ts, err = getLongPollDetails(ctx, client, accessToken, groupID)
			if err != nil {
				log.Printf("failed to refresh Long Poll details while waiting for server readiness: %v", err)
				if sleepContext(ctx, Engine().LongPollDelay) != nil {
					return nil, ctx.Err()
				}
			}
			continue
		}

		if tsValue := stringFromAny(lp.TS); tsValue != "" {
			ts = tsValue
		}

		for _, update := range lp.Updates {
			if update.Type != "message_new" {
				log.Printf("bootstrap saw VK update type=%q", update.Type)
				continue
			}
			message := update.Object.VKMessage()
			if message.PeerID < chatPeerStartID {
				if message.PeerID == 0 {
					log.Printf("bootstrap got message_new without peer_id in known fields")
				}
				continue
			}
			log.Printf("bootstrap saw VK message_new peer_id=%d text=%q", message.PeerID, truncateLogText(message.Text))
			if _, ok := announced[message.PeerID]; !ok {
				log.Printf("learned VK chat peer_id=%d from Long Poll event", message.PeerID)
				announceClientReady(ctx, client, accessToken, message.PeerID, phrase, announced)
			}
			parsed := ParseMessage(message)
			if parsed.Headers["To"] != string(TargetClient) {
				continue
			}
			if MessageType(parsed.Headers["Type"]) != MessageReadyACK {
				continue
			}
			if strings.TrimSpace(parsed.Payload) != phrase {
				continue
			}
			if _, ok := readySet[message.PeerID]; ok {
				continue
			}
			readySet[message.PeerID] = struct{}{}
			readyPeerIDs = append(readyPeerIDs, message.PeerID)
			log.Printf("server acknowledged readiness for peer_id=%d", message.PeerID)
			if collectDeadline.IsZero() {
				collectDeadline = time.Now().Add(Engine().BootstrapCollectTimeout)
				log.Printf("collecting additional shared peer IDs for %s", Engine().BootstrapCollectTimeout)
			}
		}
	}
}

func announceClientReady(ctx context.Context, client *http.Client, accessToken string, peerID int, phrase string, announced map[int]struct{}) {
	if _, ok := announced[peerID]; ok {
		return
	}
	if err := SendControlMessage(ctx, client, accessToken, peerID, TargetServer, MessageReady, bootstrapSessionID, 0, phrase); err != nil {
		log.Printf("failed to send client readiness to peer_id=%d: %v", peerID, err)
		return
	}
	announced[peerID] = struct{}{}
	log.Printf("client readiness sent to peer_id=%d", peerID)
}

func WaitForClientReady(ctx context.Context, client *http.Client, accessToken string, groupID int, phrase string) ([]int, error) {
	phrase = strings.TrimSpace(phrase)
	if phrase == "" {
		return nil, errors.New("handshake phrase must not be empty")
	}

	log.Printf("waiting for client readiness in shared VK chats for group_id=%d", groupID)

	server, key, ts, err := getLongPollDetails(ctx, client, accessToken, groupID)
	if err != nil {
		return nil, err
	}
	readySet := map[int]struct{}{}
	var readyPeerIDs []int
	var collectDeadline time.Time

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if bootstrapCollectionDone(readyPeerIDs, collectDeadline) {
			log.Printf("client readiness accepted for shared peer IDs=%v", readyPeerIDs)
			return readyPeerIDs, nil
		}

		checkURL, err := buildLongPollURLWithWait(server, key, ts, bootstrapLongPollWait(collectDeadline))
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			log.Printf("network error while waiting for client readiness: %v; retrying", err)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			continue
		}

		var lp longPollResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&lp)
		closeErr := resp.Body.Close()
		if decodeErr != nil {
			log.Printf("failed to decode client readiness Long Poll response: %v", decodeErr)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if closeErr != nil {
			return nil, closeErr
		}

		if lp.Failed != nil {
			log.Printf("Long Poll returned failed=%v while waiting for client readiness; refreshing server details", lp.Failed)
			if sleepContext(ctx, Engine().LongPollDelay) != nil {
				return nil, ctx.Err()
			}
			server, key, ts, err = getLongPollDetails(ctx, client, accessToken, groupID)
			if err != nil {
				log.Printf("failed to refresh Long Poll details while waiting for client readiness: %v", err)
				if sleepContext(ctx, Engine().LongPollDelay) != nil {
					return nil, ctx.Err()
				}
			}
			continue
		}

		if tsValue := stringFromAny(lp.TS); tsValue != "" {
			ts = tsValue
		}

		for _, update := range lp.Updates {
			if update.Type != "message_new" {
				continue
			}
			message := update.Object.VKMessage()
			if message.PeerID < chatPeerStartID {
				if message.PeerID == 0 {
					log.Printf("bootstrap got message_new without peer_id in known fields")
				}
				continue
			}
			log.Printf("bootstrap saw VK message_new peer_id=%d text=%q", message.PeerID, truncateLogText(message.Text))
			parsed := ParseMessage(message)
			if parsed.Headers["To"] != string(TargetServer) {
				continue
			}
			if MessageType(parsed.Headers["Type"]) != MessageReady {
				continue
			}
			if strings.TrimSpace(parsed.Payload) != phrase {
				continue
			}
			if err := SendControlMessage(ctx, client, accessToken, message.PeerID, TargetClient, MessageReadyACK, bootstrapSessionID, 0, phrase); err != nil {
				return nil, fmt.Errorf("send readiness acknowledgement: %w", err)
			}
			if _, ok := readySet[message.PeerID]; ok {
				continue
			}
			readySet[message.PeerID] = struct{}{}
			readyPeerIDs = append(readyPeerIDs, message.PeerID)
			log.Printf("client readiness accepted for peer_id=%d", message.PeerID)
			if collectDeadline.IsZero() {
				collectDeadline = time.Now().Add(Engine().BootstrapCollectTimeout)
				log.Printf("collecting additional shared peer IDs for %s", Engine().BootstrapCollectTimeout)
			}
		}
	}
}

type longPollDetails struct {
	Server string
	Key    string
	TS     string
}

type longPollResponse struct {
	TS      any        `json:"ts"`
	Updates []lpUpdate `json:"updates"`
	Failed  any        `json:"failed"`
}

type lpUpdate struct {
	Type   string   `json:"type"`
	Object lpObject `json:"object"`
}

type lpObject struct {
	Message     *vkMessage     `json:"message"`
	PeerID      int            `json:"peer_id"`
	Text        string         `json:"text"`
	Attachments []vkAttachment `json:"attachments"`
}

func (o lpObject) VKMessage() vkMessage {
	if o.Message != nil {
		return *o.Message
	}
	return vkMessage{
		PeerID:      o.PeerID,
		Text:        o.Text,
		Attachments: o.Attachments,
	}
}

type vkMessage struct {
	PeerID      int            `json:"peer_id"`
	Text        string         `json:"text"`
	Attachments []vkAttachment `json:"attachments"`
}

type vkAttachment struct {
	Type string `json:"type"`
	Doc  *vkDoc `json:"doc"`
}

type vkDoc struct {
	URL string `json:"url"`
}

func getLongPollDetails(ctx context.Context, client *http.Client, accessToken string, groupID int) (string, string, string, error) {
	resp, err := APICall(ctx, client, "groups.getLongPollServer", url.Values{
		"group_id": {strconv.Itoa(groupID)},
	}, accessToken)
	if err != nil {
		return "", "", "", err
	}

	response, ok := resp["response"].(map[string]any)
	if !ok {
		return "", "", "", errors.New("groups.getLongPollServer response is missing response object")
	}
	details := longPollDetails{
		Server: stringFromAny(response["server"]),
		Key:    stringFromAny(response["key"]),
		TS:     stringFromAny(response["ts"]),
	}
	if details.Server == "" || details.Key == "" || details.TS == "" {
		return "", "", "", fmt.Errorf("invalid Long Poll details: %+v", response)
	}
	return details.Server, details.Key, details.TS, nil
}

func buildLongPollURL(server, key, ts string) (string, error) {
	return buildLongPollURLWithWait(server, key, ts, Engine().LongPollWait)
}

func buildLongPollURLWithWait(server, key, ts string, wait int) (string, error) {
	parsed, err := url.Parse(server)
	if err != nil {
		return "", err
	}
	if wait <= 0 {
		wait = Engine().LongPollWait
	}
	query := parsed.Query()
	query.Set("act", "a_check")
	query.Set("key", key)
	query.Set("ts", ts)
	query.Set("wait", strconv.Itoa(wait))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func bootstrapLongPollWait(deadline time.Time) int {
	defaultWait := Engine().LongPollWait
	if deadline.IsZero() {
		return defaultWait
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 1
	}
	wait := int((remaining + time.Second - time.Nanosecond) / time.Second)
	if wait < 1 {
		wait = 1
	}
	if defaultWait > 0 && wait > defaultWait {
		return defaultWait
	}
	return wait
}

func bootstrapCollectionDone(peerIDs []int, deadline time.Time) bool {
	return len(peerIDs) > 0 && !deadline.IsZero() && time.Now().After(deadline)
}

func truncateLogText(text string) string {
	const limit = 120
	text = strings.TrimSpace(text)
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stringFromAny(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func intFromAny(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		parsed, _ := v.Int64()
		return int(parsed)
	default:
		return 0
	}
}
