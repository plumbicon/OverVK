package overvk

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type RuntimeMode string

const (
	ModeClient RuntimeMode = "client"
	ModeServer RuntimeMode = "server"

	DefaultHandshakePhrase = "/overvk-peer"
)

type RuntimeConfig struct {
	Mode            RuntimeMode
	Token           string
	Key             string
	GroupID         int
	PeerIDs         []int
	HandshakePhrase string
	Verbose         bool
	TLSMITM         bool
	MITMCACert      string
	MITMCAKey       string
	ProxyType       ProxyType
	Engine          EngineConfig
}

type rawConfig struct {
	Mode            RuntimeMode     `yaml:"mode"`
	Token           string          `yaml:"token"`
	Key             string          `yaml:"key"`
	GroupID         int             `yaml:"group"`
	HandshakePhrase string          `yaml:"handshake_phrase"`
	DiscoveryPhrase string          `yaml:"discovery_phrase"`
	Verbose         bool            `yaml:"verbose"`
	TLSMITM         bool            `yaml:"tls_mitm"`
	MITMCACert      string          `yaml:"mitm_ca_cert"`
	MITMCAKey       string          `yaml:"mitm_ca_key"`
	ProxyType       string          `yaml:"proxy_type"`
	Engine          rawEngineConfig `yaml:"engine"`
}

type rawEngineConfig struct {
	APIVersion              string `yaml:"api_version"`
	MaxChunkSize            int    `yaml:"max_chunk_size"`
	ChunkTimeout            string `yaml:"chunk_timeout"`
	SenderWorkers           int    `yaml:"sender_workers"`
	VKMessageMaxLength    int    `yaml:"vk_message_max_length"`
	TextMessageThreshold  int    `yaml:"text_message_threshold"`
	UploadURLCacheTTL       string `yaml:"upload_url_cache_ttl"`
	MaxPacketBufferSize     int    `yaml:"max_packet_buffer_size"`
	PacketBufferTimeout     string `yaml:"packet_buffer_timeout"`
	SenderQueueSize         int    `yaml:"sender_queue_size"`
	SOCKSHost               string `yaml:"socks_host"`
	SOCKSPort               int    `yaml:"socks_port"`
	EnableSessionStatistics *bool  `yaml:"enable_session_statistics"`
	HTTPTimeout             string `yaml:"http_timeout"`
	ACKWait                 string `yaml:"ack_wait"`
	LongPollWait            int    `yaml:"long_poll_wait"`
	LongPollDelay           string `yaml:"long_poll_delay"`
	BootstrapPeerScanLimit  int    `yaml:"bootstrap_peer_scan_limit"`
	BootstrapCollectTimeout string `yaml:"bootstrap_collect_timeout"`
}

func LoadRuntimeConfig(path string) (RuntimeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RuntimeConfig{}, err
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return RuntimeConfig{}, err
	}

	engine, err := raw.Engine.withDefaults()
	if err != nil {
		return RuntimeConfig{}, err
	}

	handshakePhrase := strings.TrimSpace(raw.HandshakePhrase)
	if handshakePhrase == "" {
		handshakePhrase = strings.TrimSpace(raw.DiscoveryPhrase)
	}
	if handshakePhrase == "" {
		handshakePhrase = DefaultHandshakePhrase
	}

	proxyType := ProxyType(strings.TrimSpace(raw.ProxyType))
	if proxyType == "" {
		proxyType = ProxySOCKS5
	}
	tlsMITM := raw.TLSMITM
	if proxyType == ProxyHTTP || proxyType == ProxyHybrid {
		tlsMITM = true
	}

	config := RuntimeConfig{
		Mode:            raw.Mode,
		Token:           strings.TrimSpace(raw.Token),
		Key:             strings.TrimSpace(raw.Key),
		GroupID:         raw.GroupID,
		HandshakePhrase: handshakePhrase,
		Verbose:         raw.Verbose,
		TLSMITM:         tlsMITM,
		MITMCACert:      strings.TrimSpace(raw.MITMCACert),
		MITMCAKey:       strings.TrimSpace(raw.MITMCAKey),
		ProxyType:       proxyType,
		Engine:          engine,
	}
	if err := config.Validate(); err != nil {
		return RuntimeConfig{}, err
	}
	return config, nil
}

func (c RuntimeConfig) Validate() error {
	switch c.Mode {
	case ModeClient, ModeServer:
	default:
		return fmt.Errorf("mode must be %q or %q", ModeClient, ModeServer)
	}
	if c.Token == "" {
		return fmt.Errorf("token is required")
	}
	if c.GroupID <= 0 {
		return fmt.Errorf("group must be a positive integer")
	}
	if strings.TrimSpace(c.HandshakePhrase) == "" {
		return fmt.Errorf("handshake_phrase must not be empty")
	}
	return nil
}

func (raw rawEngineConfig) withDefaults() (EngineConfig, error) {
	engine := DefaultEngineConfig()

	if raw.APIVersion != "" {
		engine.APIVersion = raw.APIVersion
	}
	if raw.MaxChunkSize > 0 {
		engine.MaxChunkSize = raw.MaxChunkSize
	}
	if raw.ChunkTimeout != "" {
		duration, err := time.ParseDuration(raw.ChunkTimeout)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.chunk_timeout: %w", err)
		}
		engine.ChunkTimeout = duration
	}
	if raw.SenderWorkers > 0 {
		engine.SenderWorkers = raw.SenderWorkers
	}
	if raw.VKMessageMaxLength > 0 {
		engine.VKMessageMaxLength = raw.VKMessageMaxLength
	}
	if raw.TextMessageThreshold > 0 {
		engine.TextMessageThreshold = raw.TextMessageThreshold
	}
	if raw.UploadURLCacheTTL != "" {
		duration, err := time.ParseDuration(raw.UploadURLCacheTTL)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.upload_url_cache_ttl: %w", err)
		}
		engine.UploadURLCacheTTL = duration
	}
	if raw.MaxPacketBufferSize > 0 {
		engine.MaxPacketBufferSize = raw.MaxPacketBufferSize
	}
	if raw.PacketBufferTimeout != "" {
		duration, err := time.ParseDuration(raw.PacketBufferTimeout)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.packet_buffer_timeout: %w", err)
		}
		engine.PacketBufferTimeout = duration
	}
	if raw.SenderQueueSize > 0 {
		engine.SenderQueueSize = raw.SenderQueueSize
	}
	if raw.SOCKSHost != "" {
		engine.SOCKSHost = raw.SOCKSHost
	}
	if raw.SOCKSPort > 0 {
		engine.SOCKSPort = raw.SOCKSPort
	}
	if raw.EnableSessionStatistics != nil {
		engine.EnableSessionStatistics = *raw.EnableSessionStatistics
	}
	if raw.HTTPTimeout != "" {
		duration, err := time.ParseDuration(raw.HTTPTimeout)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.http_timeout: %w", err)
		}
		engine.HTTPTimeout = duration
	}
	if raw.ACKWait != "" {
		duration, err := time.ParseDuration(raw.ACKWait)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.ack_wait: %w", err)
		}
		engine.ACKWait = duration
	}
	if raw.LongPollWait > 0 {
		engine.LongPollWait = raw.LongPollWait
	}
	if raw.LongPollDelay != "" {
		duration, err := time.ParseDuration(raw.LongPollDelay)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.long_poll_delay: %w", err)
		}
		engine.LongPollDelay = duration
	}
	if raw.BootstrapPeerScanLimit > 0 {
		engine.BootstrapPeerScanLimit = raw.BootstrapPeerScanLimit
	}
	if raw.BootstrapCollectTimeout != "" {
		duration, err := time.ParseDuration(raw.BootstrapCollectTimeout)
		if err != nil {
			return EngineConfig{}, fmt.Errorf("engine.bootstrap_collect_timeout: %w", err)
		}
		engine.BootstrapCollectTimeout = duration
	}

	return engine, nil
}
