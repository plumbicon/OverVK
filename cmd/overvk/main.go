package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"overvk/internal/overvk"
)

type clientSession struct {
	conn          net.Conn
	ack           chan struct{}
	ackOnce       sync.Once
	incoming      chan overvk.Packet
	writerStarted bool
	mu            sync.Mutex
}

type clientApp struct {
	config     overvk.RuntimeConfig
	httpClient *http.Client
	rotator    *overvk.ChatRotator
	sender     *overvk.SenderQueue
	multipart  *overvk.MultipartStore

	mu       sync.RWMutex
	sessions map[string]*clientSession
}

type serverSession struct {
	host          string
	port          int
	incoming      chan overvk.Packet
	writerStarted bool
	mu            sync.Mutex
}

type serverApp struct {
	config     overvk.RuntimeConfig
	httpClient *http.Client
	rotator    *overvk.ChatRotator
	sender     *overvk.SenderQueue
	multipart  *overvk.MultipartStore

	mu       sync.RWMutex
	sessions map[string]*serverSession
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s path/to/config.yaml\n", os.Args[0])
		os.Exit(2)
	}

	config, err := overvk.LoadRuntimeConfig(os.Args[1])
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}
	overvk.ConfigureEngine(config.Engine)
	if err := overvk.ConfigureCipher(config.Key); err != nil {
		log.Fatalf("encryption setup failed: %v", err)
	}
	if config.Key != "" {
		log.Println("Encryption enabled (AES-256-GCM)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch config.Mode {
	case overvk.ModeClient:
		if err := prepareClientPeer(ctx, &config); err != nil {
			log.Fatalf("client bootstrap failed: %v", err)
		}
		runClient(ctx, config)
	case overvk.ModeServer:
		if err := prepareServerPeer(ctx, &config); err != nil {
			log.Fatalf("server bootstrap failed: %v", err)
		}
		runServer(ctx, config)
	default:
		log.Fatalf("unsupported mode: %s", config.Mode)
	}
}

func prepareClientPeer(ctx context.Context, config *overvk.RuntimeConfig) error {
	httpClient := overvk.NewHTTPClient()
	peerIDs, err := overvk.WaitForServerReady(ctx, httpClient, config.Token, config.GroupID, config.HandshakePhrase)
	if err != nil {
		return err
	}
	config.PeerIDs = peerIDs
	log.Printf("using server-ready peer IDs=%v for this run", peerIDs)
	return nil
}

func prepareServerPeer(ctx context.Context, config *overvk.RuntimeConfig) error {
	httpClient := overvk.NewHTTPClient()
	peerIDs, err := overvk.WaitForClientReady(ctx, httpClient, config.Token, config.GroupID, config.HandshakePhrase)
	if err != nil {
		return err
	}
	config.PeerIDs = peerIDs
	log.Printf("using client-ready peer IDs=%v for this run", peerIDs)
	return nil
}

func runClient(rootCtx context.Context, config overvk.RuntimeConfig) {
	engine := config.Engine
	httpClient := overvk.NewHTTPClient()
	workerCtx, stopWorkers := context.WithCancel(context.Background())

	app := &clientApp{
		config:     config,
		httpClient: httpClient,
		rotator:    overvk.NewChatRotator(config.PeerIDs),
		sender:     overvk.NewSenderQueue(engine.SenderQueueSize),
		multipart:  overvk.NewMultipartStore(),
		sessions:   map[string]*clientSession{},
	}

	workerDone := overvk.StartSenderWorkers(workerCtx, app.sender, httpClient, config.Token, app.rotator, engine.SenderWorkers)

	go func() {
		if err := overvk.StartLongPollListener(rootCtx, httpClient, config.Token, config.GroupID, config.PeerIDs, app.handleServerMessage); err != nil {
			log.Printf("Long Poll listener stopped: %v", err)
		}
	}()

	address := net.JoinHostPort(engine.SOCKSHost, strconv.Itoa(engine.SOCKSPort))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to start SOCKS5 proxy on %s: %v", address, err)
	}
	log.Printf("SOCKS5 local proxy started on %s", address)

	go func() {
		<-rootCtx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if rootCtx.Err() != nil {
				break
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go app.handleBrowserConnection(rootCtx, conn)
	}

	log.Print("Shutting down client: waiting for sender queue")
	app.sender.Wait()
	overvk.StopSenderWorkers(stopWorkers, workerDone)
	overvk.ReportGlobalStatistics()
}

func (a *clientApp) handleServerMessage(ctx context.Context, message overvk.ParsedMessage) {
	sessionID := message.Headers["SessionID"]
	if sessionID == "" || message.Headers["To"] != string(overvk.TargetClient) {
		return
	}

	msgType := overvk.MessageType(message.Headers["Type"])
	switch msgType {
	case overvk.MessageACK:
		session := a.getSession(sessionID)
		if session != nil {
			session.ackOnce.Do(func() { close(session.ack) })
		}
	case overvk.MessageData:
		session := a.getSession(sessionID)
		if session == nil {
			return
		}
		if err := overvk.ProcessDataMessage(ctx, a.httpClient, message, a.multipart, session.incoming, a.config.Verbose); err != nil {
			log.Printf("[%s] failed to process server DATA message: %v", sessionID, err)
			return
		}
		session.mu.Lock()
		if !session.writerStarted {
			session.writerStarted = true
			go overvk.WriteOrderedPackets(ctx, sessionID, session.conn, session.incoming, a.config.Verbose)
		}
		session.mu.Unlock()
	case overvk.MessageClose:
		session := a.getSession(sessionID)
		if session != nil {
			select {
			case session.incoming <- overvk.Packet{Close: true}:
			default:
			}
		}
		a.deleteSession(sessionID)
		overvk.CleanupSessionMetrics(sessionID)
	}
}

func (a *clientApp) handleBrowserConnection(ctx context.Context, conn net.Conn) {
	sessionID := overvk.NewSessionID()
	session := &clientSession{
		conn:     conn,
		ack:      make(chan struct{}),
		incoming: make(chan overvk.Packet, a.config.Engine.MaxPacketBufferSize*2),
	}
	a.setSession(sessionID, session)
	defer func() {
		a.deleteSession(sessionID)
		a.multipart.Cleanup(sessionID)
		_ = conn.Close()
		log.Printf("[%s] Browser connection closed", sessionID)
	}()

	log.Printf("[%s] New browser connection", sessionID)
	host, port, err := negotiateSOCKS5(conn)
	if err != nil {
		log.Printf("[%s] SOCKS5 negotiation failed: %v", sessionID, err)
		return
	}
	log.Printf("[%s] SOCKS5 CONNECT for %s:%d", sessionID, host, port)

	peerID := a.rotator.Next()
	connectPayload := net.JoinHostPort(host, strconv.Itoa(port))
	if err := overvk.SendControlMessage(ctx, a.httpClient, a.config.Token, peerID, overvk.TargetServer, overvk.MessageConnect, sessionID, 0, connectPayload); err != nil {
		log.Printf("[%s] failed to send CONNECT: %v", sessionID, err)
		_ = writeSOCKS5Reply(conn, 0x01)
		return
	}

	log.Printf("[%s] Waiting for session acknowledgement", sessionID)
	select {
	case <-session.ack:
	case <-ctx.Done():
		return
	case <-timeAfter(a.config.Engine.ACKWait):
		log.Printf("[%s] Session acknowledgement timeout", sessionID)
		_ = writeSOCKS5Reply(conn, 0x01)
		return
	}

	if err := writeSOCKS5Reply(conn, 0x00); err != nil {
		log.Printf("[%s] failed to write SOCKS5 success reply: %v", sessionID, err)
		return
	}
	log.Printf("[%s] Tunnel established", sessionID)

	nextSequence := overvk.DataSenderHandler(ctx, "Uplink", sessionID, conn, overvk.TargetServer, a.sender)
	closePeerID := a.rotator.Next()
	if err := overvk.SendControlMessage(context.Background(), a.httpClient, a.config.Token, closePeerID, overvk.TargetServer, overvk.MessageClose, sessionID, nextSequence, ""); err != nil {
		log.Printf("[%s] failed to send CLOSE: %v", sessionID, err)
	}
}

func runServer(rootCtx context.Context, config overvk.RuntimeConfig) {
	engine := config.Engine
	httpClient := overvk.NewHTTPClient()
	workerCtx, stopWorkers := context.WithCancel(context.Background())

	app := &serverApp{
		config:     config,
		httpClient: httpClient,
		rotator:    overvk.NewChatRotator(config.PeerIDs),
		sender:     overvk.NewSenderQueue(engine.SenderQueueSize),
		multipart:  overvk.NewMultipartStore(),
		sessions:   map[string]*serverSession{},
	}

	workerDone := overvk.StartSenderWorkers(workerCtx, app.sender, httpClient, config.Token, app.rotator, engine.SenderWorkers)

	if err := overvk.StartLongPollListener(rootCtx, httpClient, config.Token, config.GroupID, config.PeerIDs, app.handleClientMessage); err != nil {
		log.Printf("Long Poll listener stopped: %v", err)
	}

	log.Print("Shutting down server: waiting for sender queue")
	app.sender.Wait()
	overvk.StopSenderWorkers(stopWorkers, workerDone)
	overvk.ReportGlobalStatistics()
}

func (a *serverApp) handleClientMessage(ctx context.Context, message overvk.ParsedMessage) {
	sessionID := message.Headers["SessionID"]
	if sessionID == "" || message.Headers["To"] != string(overvk.TargetServer) {
		return
	}

	msgType := overvk.MessageType(message.Headers["Type"])
	switch msgType {
	case overvk.MessageConnect:
		host, port, err := parseConnectPayload(message.Payload)
		if err != nil {
			log.Printf("[%s] invalid CONNECT payload %q: %v", sessionID, message.Payload, err)
			return
		}
		if a.getSession(sessionID) != nil {
			return
		}
		session := &serverSession{
			host:     host,
			port:     port,
			incoming: make(chan overvk.Packet, a.config.Engine.MaxPacketBufferSize*2),
		}
		a.setSession(sessionID, session)

		peerID := a.rotator.Next()
		if err := overvk.SendControlMessage(ctx, a.httpClient, a.config.Token, peerID, overvk.TargetClient, overvk.MessageACK, sessionID, 0, ""); err != nil {
			log.Printf("[%s] failed to ACK connect request for %s:%d: %v", sessionID, host, port, err)
			a.deleteSession(sessionID)
			return
		}
		log.Printf("[%s] Acknowledged connect request for %s:%d", sessionID, host, port)

	case overvk.MessageData:
		session := a.getSession(sessionID)
		if session == nil {
			return
		}
		if err := overvk.ProcessDataMessage(ctx, a.httpClient, message, a.multipart, session.incoming, a.config.Verbose); err != nil {
			log.Printf("[%s] failed to process client DATA message: %v", sessionID, err)
			return
		}
		session.mu.Lock()
		if !session.writerStarted {
			session.writerStarted = true
			go a.serverWriterTask(ctx, sessionID, session)
		}
		session.mu.Unlock()

	case overvk.MessageClose:
		session := a.popSession(sessionID)
		if session != nil {
			select {
			case session.incoming <- overvk.Packet{Close: true}:
			default:
			}
		}
		a.multipart.Cleanup(sessionID)
		overvk.CleanupSessionMetrics(sessionID)
		log.Printf("[%s] Session terminated by client", sessionID)
	}
}

func (a *serverApp) serverWriterTask(ctx context.Context, sessionID string, session *serverSession) {
	address := net.JoinHostPort(session.host, strconv.Itoa(session.port))
	log.Printf("[%s] Writer task started, connecting to %s", sessionID, address)

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		log.Printf("[%s] connection failed in server writer task: %v", sessionID, err)
		a.deleteSession(sessionID)
		return
	}

	go a.downlinkHandler(ctx, sessionID, conn)
	overvk.WriteOrderedPackets(ctx, sessionID, conn, session.incoming, a.config.Verbose)
	a.deleteSession(sessionID)
	log.Printf("[%s] Writer task stopped", sessionID)
}

func (a *serverApp) downlinkHandler(ctx context.Context, sessionID string, conn net.Conn) {
	nextSequence := overvk.DataSenderHandler(ctx, "Downlink", sessionID, conn, overvk.TargetClient, a.sender)
	peerID := a.rotator.Next()
	if err := overvk.SendControlMessage(context.Background(), a.httpClient, a.config.Token, peerID, overvk.TargetClient, overvk.MessageClose, sessionID, nextSequence+1, ""); err != nil {
		log.Printf("[%s] failed to send downlink CLOSE: %v", sessionID, err)
	}
}

func negotiateSOCKS5(conn net.Conn) (string, int, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", 0, err
	}
	if header[0] != overvk.SOCKSVersion {
		return "", 0, fmt.Errorf("unsupported SOCKS version %d", header[0])
	}

	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", 0, err
	}
	if _, err := conn.Write([]byte{overvk.SOCKSVersion, 0x00}); err != nil {
		return "", 0, err
	}

	requestHeader := make([]byte, 4)
	if _, err := io.ReadFull(conn, requestHeader); err != nil {
		return "", 0, err
	}
	if requestHeader[0] != overvk.SOCKSVersion {
		return "", 0, fmt.Errorf("unsupported request SOCKS version %d", requestHeader[0])
	}
	if requestHeader[1] != overvk.SOCKSCmdConnect {
		_ = writeSOCKS5Reply(conn, 0x07)
		return "", 0, fmt.Errorf("unsupported SOCKS command %d", requestHeader[1])
	}

	host, err := readSOCKSAddress(conn, requestHeader[3])
	if err != nil {
		_ = writeSOCKS5Reply(conn, 0x08)
		return "", 0, err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", 0, err
	}
	port := int(binary.BigEndian.Uint16(portBytes))
	return host, port, nil
}

func readSOCKSAddress(conn net.Conn, addressType byte) (string, error) {
	switch addressType {
	case overvk.SOCKSAtypDomainName:
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return "", err
		}
		domain := make([]byte, int(length[0]))
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", err
		}
		return string(domain), nil
	case overvk.SOCKSAtypIPv4:
		ip := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", err
		}
		return net.IP(ip).String(), nil
	case overvk.SOCKSAtypIPv6:
		ip := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", err
		}
		return net.IP(ip).String(), nil
	default:
		return "", fmt.Errorf("unsupported address type %d", addressType)
	}
}

func writeSOCKS5Reply(conn net.Conn, reply byte) error {
	_, err := conn.Write([]byte{overvk.SOCKSVersion, reply, 0x00, overvk.SOCKSAtypIPv4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	return err
}

func parseConnectPayload(payload string) (string, int, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return "", 0, fmt.Errorf("empty payload")
	}

	host, portText, err := net.SplitHostPort(payload)
	if err != nil {
		index := strings.LastIndex(payload, ":")
		if index <= 0 || index == len(payload)-1 {
			return "", 0, err
		}
		host = payload[:index]
		portText = payload[index+1:]
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port %d", port)
	}
	return strings.Trim(host, "[]"), port, nil
}

func timeAfter(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

func (a *clientApp) getSession(sessionID string) *clientSession {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.sessions[sessionID]
}

func (a *clientApp) setSession(sessionID string, session *clientSession) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessions[sessionID] = session
}

func (a *clientApp) deleteSession(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, sessionID)
}

func (a *serverApp) getSession(sessionID string) *serverSession {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.sessions[sessionID]
}

func (a *serverApp) setSession(sessionID string, session *serverSession) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessions[sessionID] = session
}

func (a *serverApp) deleteSession(sessionID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sessions, sessionID)
}

func (a *serverApp) popSession(sessionID string) *serverSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	session := a.sessions[sessionID]
	delete(a.sessions, sessionID)
	return session
}
