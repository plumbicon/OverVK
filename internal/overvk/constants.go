package overvk

import (
	"sync"
	"time"
)

const (
	SOCKSVersion = 5
)

type EngineConfig struct {
	APIVersion              string
	MaxChunkSize            int
	ChunkTimeout            time.Duration
	SenderWorkers           int
	VKMessageMaxLength      int
	MaxTextMessagePayload   int
	TextMessageThreshold    int
	UploadURLCacheTTL       time.Duration
	MaxPacketBufferSize     int
	PacketBufferTimeout     time.Duration
	SenderQueueSize         int
	SOCKSHost               string
	SOCKSPort               int
	EnableSessionStatistics bool
	HTTPTimeout             time.Duration
	ACKWait                 time.Duration
	LongPollWait            int
	LongPollDelay           time.Duration
	BootstrapPeerScanLimit  int
	BootstrapCollectTimeout time.Duration
}

func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		APIVersion:              "5.131",
		MaxChunkSize:            1024 * 1024,
		ChunkTimeout:            150 * time.Millisecond,
		SenderWorkers:           16,
		VKMessageMaxLength:      4096,
		MaxTextMessagePayload:   3000,
		TextMessageThreshold:    6 * 1024,
		UploadURLCacheTTL:       5 * time.Minute,
		MaxPacketBufferSize:     100,
		PacketBufferTimeout:     30 * time.Second,
		SenderQueueSize:         1000,
		SOCKSHost:               "127.0.0.1",
		SOCKSPort:               8888,
		EnableSessionStatistics: true,
		HTTPTimeout:             90 * time.Second,
		ACKWait:                 60 * time.Second,
		LongPollWait:            25,
		LongPollDelay:           5 * time.Second,
		BootstrapPeerScanLimit:  500,
		BootstrapCollectTimeout: 3 * time.Second,
	}
}

var (
	engineMu     sync.RWMutex
	activeEngine = DefaultEngineConfig()
)

func ConfigureEngine(config EngineConfig) {
	engineMu.Lock()
	defer engineMu.Unlock()
	activeEngine = config
}

func Engine() EngineConfig {
	engineMu.RLock()
	defer engineMu.RUnlock()
	return activeEngine
}

type MessageType string

const (
	MessageConnect  MessageType = "connect"
	MessageData     MessageType = "data"
	MessageACK      MessageType = "ack"
	MessageClose    MessageType = "close"
	MessageReady    MessageType = "ready"
	MessageReadyACK MessageType = "ready_ack"
)

type Target string

const (
	TargetClient Target = "client"
	TargetServer Target = "server"
)

const (
	SOCKSCmdConnect     = 1
	SOCKSAtypIPv4       = 1
	SOCKSAtypDomainName = 3
	SOCKSAtypIPv6       = 4
	defaultDialTimeout  = 30 * time.Second
)
