package overvk

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type HTTPRequest struct {
	Method  string            `json:"m"`
	URL     string            `json:"u"`
	Headers map[string]string `json:"h,omitempty"`
	Body    string            `json:"b,omitempty"`
}

type HTTPResponse struct {
	Status  int               `json:"s"`
	Headers map[string]string `json:"h,omitempty"`
	Body    string            `json:"b,omitempty"`
	BodyDoc bool              `json:"d,omitempty"`
}

func SerializeHTTPRequest(req *http.Request) ([]byte, error) {
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read request body: %w", err)
		}
		req.Body.Close()
	}

	hr := HTTPRequest{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: make(map[string]string),
	}
	for key := range req.Header {
		hr.Headers[key] = req.Header.Get(key)
	}
	if len(bodyBytes) > 0 {
		hr.Body = base64.StdEncoding.EncodeToString(bodyBytes)
	}

	return json.Marshal(hr)
}

func ExecuteHTTPRequest(ctx context.Context, reqData []byte) (statusCode int, headers map[string]string, body []byte, err error) {
	var hr HTTPRequest
	if err := json.Unmarshal(reqData, &hr); err != nil {
		return 0, nil, nil, fmt.Errorf("unmarshal http request: %w", err)
	}

	var bodyReader io.Reader
	if hr.Body != "" {
		decoded, err := base64.StdEncoding.DecodeString(hr.Body)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("decode request body: %w", err)
		}
		bodyReader = bytes.NewReader(decoded)
	}

	httpReq, err := http.NewRequestWithContext(ctx, hr.Method, hr.URL, bodyReader)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("create http request: %w", err)
	}
	for key, value := range hr.Headers {
		if strings.EqualFold(key, "Accept-Encoding") {
			continue
		}
		httpReq.Header.Set(key, value)
	}

	client := &http.Client{
		Timeout: Engine().HTTPTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("execute http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(bytes.NewReader(respBody))
		if err == nil {
			decompressed, err := io.ReadAll(gz)
			gz.Close()
			if err == nil {
				respBody = decompressed
				resp.Header.Del("Content-Encoding")
				resp.Header.Set("Content-Length", strconv.Itoa(len(respBody)))
			}
		}
	}

	respHeaders := make(map[string]string)
	for key := range resp.Header {
		respHeaders[key] = resp.Header.Get(key)
	}

	return resp.StatusCode, respHeaders, respBody, nil
}

func SerializeHTTPResponse(status int, headers map[string]string, body []byte) ([]byte, error) {
	hr := HTTPResponse{
		Status:  status,
		Headers: headers,
		Body:    base64.StdEncoding.EncodeToString(body),
	}
	return json.Marshal(hr)
}

func DeserializeHTTPResponse(data []byte) (int, map[string]string, []byte, error) {
	var hr HTTPResponse
	if err := json.Unmarshal(data, &hr); err != nil {
		return 0, nil, nil, fmt.Errorf("unmarshal http response: %w", err)
	}
	body, err := base64.StdEncoding.DecodeString(hr.Body)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("decode response body: %w", err)
	}
	return hr.Status, hr.Headers, body, nil
}

func WriteHTTPResponse(conn net.Conn, status int, headers map[string]string, body []byte) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", status, http.StatusText(status)))
	for key, value := range headers {
		if strings.EqualFold(key, "Transfer-Encoding") {
			continue
		}
		buf.WriteString(key)
		buf.WriteString(": ")
		buf.WriteString(value)
		buf.WriteString("\r\n")
	}
	buf.WriteString("Content-Length: ")
	buf.WriteString(strconv.Itoa(len(body)))
	buf.WriteString("\r\n")
	buf.WriteString("Connection: close\r\n")
	buf.WriteString("\r\n")
	buf.Write(body)
	_, err := conn.Write(buf.Bytes())
	return err
}

func HandleHTTPConnect(conn net.Conn, host string, port int) error {
	_, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	if err != nil {
		return fmt.Errorf("write CONNECT response: %w", err)
	}
	return nil
}

func ReadHTTPRequest(conn net.Conn) (method, host string, port int, req *http.Request, err error) {
	reader := bufio.NewReader(conn)
	req, err = http.ReadRequest(reader)
	if err != nil {
		return "", "", 0, nil, err
	}

	if req.Method == http.MethodConnect {
		hostPort := req.Host
		h, p, splitErr := net.SplitHostPort(hostPort)
		if splitErr != nil {
			h = hostPort
			p = "443"
		}
		portNum, _ := strconv.Atoi(p)
		return http.MethodConnect, h, portNum, req, nil
	}

	if !req.URL.IsAbs() {
		req.URL.Scheme = "http"
		req.URL.Host = req.Host
	}
	h, p, splitErr := net.SplitHostPort(req.Host)
	if splitErr != nil {
		h = req.Host
		p = "80"
	}
	portNum, _ := strconv.Atoi(p)

	return req.Method, h, portNum, req, nil
}

func ReadHTTPRequestFromTLS(tlsConn net.Conn, host string, isTLS bool) (*http.Request, error) {
	if err := tlsConn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(tlsConn)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return nil, err
	}
	if err := tlsConn.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}

	scheme := "http"
	if isTLS {
		scheme = "https"
	}
	if !req.URL.IsAbs() {
		req.URL.Scheme = scheme
		req.URL.Host = host
	}
	return req, nil
}

func SendHTTPRequestMessage(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, rotator *ChatRotator, sessionID string, reqData []byte) error {
	return sendHTTPMessage(ctx, client, accessToken, chatPeerID, rotator, TargetServer, MessageHTTPReq, sessionID, reqData)
}

func SendHTTPResponseMessage(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, rotator *ChatRotator, sessionID string, respData []byte) error {
	return sendHTTPMessage(ctx, client, accessToken, chatPeerID, rotator, TargetClient, MessageHTTPResp, sessionID, respData)
}

func sendHTTPMessage(ctx context.Context, client *http.Client, accessToken string, chatPeerID int, rotator *ChatRotator, to Target, msgType MessageType, sessionID string, data []byte) error {
	engine := Engine()
	payload := base64.StdEncoding.EncodeToString(data)
	message := buildMessage(to, msgType, sessionID, 0, payload, "")

	encrypted, err := EncryptMessageText(message)
	if err != nil {
		return fmt.Errorf("encrypt %s: %w", msgType, err)
	}

	if len(encrypted) <= engine.VKMessageMaxLength {
		_, err := APICall(ctx, client, "messages.send", url.Values{
			"peer_id":   {strconv.Itoa(chatPeerID)},
			"message":   {encrypted},
			"random_id": {randomID()},
		}, accessToken)
		return err
	}

	encData, err := Encrypt(data)
	if err != nil {
		return fmt.Errorf("encrypt document data: %w", err)
	}
	headerBlock, err := EncryptMessageText(buildHeaderBlock(to, msgType, sessionID, 0, ""))
	if err != nil {
		return fmt.Errorf("encrypt header: %w", err)
	}

	const maxUploadRetries = 5
	var lastErr error
	peerID := chatPeerID

	for attempt := 0; attempt < maxUploadRetries; attempt++ {
		if attempt > 0 {
			peerID = rotator.Next()
			invalidateUploadURL(accessToken, peerID, 0)
			log.Printf("[%s] Retrying HTTP document upload via peer %d (attempt %d/%d)", sessionID, peerID, attempt+1, maxUploadRetries)
		}

		uploadURL, _, err := getUploadURL(ctx, client, accessToken, peerID, sessionID)
		if err != nil {
			lastErr = err
			continue
		}

		result, err := uploadDocument(ctx, client, uploadURL, encData)
		if err != nil {
			invalidateUploadURL(accessToken, peerID, 0)
			lastErr = err
			continue
		}

		fileValue := stringFromAny(result["file"])
		if fileValue == "" {
			invalidateUploadURL(accessToken, peerID, 0)
			lastErr = fmt.Errorf("upload response missing file field")
			continue
		}

		saveResp, err := APICall(ctx, client, "docs.save", url.Values{"file": {fileValue}}, accessToken)
		if err != nil {
			lastErr = fmt.Errorf("docs.save failed: %w", err)
			continue
		}
		attachment, err := documentAttachmentString(saveResp)
		if err != nil {
			return fmt.Errorf("parse docs.save response: %w", err)
		}

		_, err = APICall(ctx, client, "messages.send", url.Values{
			"peer_id":    {strconv.Itoa(peerID)},
			"message":    {headerBlock},
			"attachment": {attachment},
			"random_id":  {randomID()},
		}, accessToken)
		if err != nil {
			return fmt.Errorf("send document message: %w", err)
		}
		return nil
	}

	return lastErr
}

func ExtractHTTPResponsePayload(ctx context.Context, client *http.Client, message ParsedMessage) ([]byte, error) {
	if message.Attachment != nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, message.Attachment.URL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return Decrypt(raw)
	}

	if message.Payload == "" {
		return nil, fmt.Errorf("http message without payload or attachment")
	}
	return base64.StdEncoding.DecodeString(message.Payload)
}
