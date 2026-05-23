package overvk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRuntimeConfigExamples(t *testing.T) {
	tests := []struct {
		path string
		mode RuntimeMode
	}{
		{path: "../../client.config.example.yaml", mode: ModeClient},
		{path: "../../server.config.example.yaml", mode: ModeServer},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			config, err := LoadRuntimeConfig(tt.path)
			if err != nil {
				t.Fatalf("LoadRuntimeConfig() error = %v", err)
			}
			if config.Mode != tt.mode {
				t.Fatalf("mode = %q, want %q", config.Mode, tt.mode)
			}
			if len(config.PeerIDs) != 0 {
				t.Fatalf("peer count = %d, want 0 before discovery", len(config.PeerIDs))
			}
			if config.HandshakePhrase != DefaultHandshakePhrase {
				t.Fatalf("HandshakePhrase = %q, want %q", config.HandshakePhrase, DefaultHandshakePhrase)
			}
		})
	}
}

func TestLoadRuntimeConfigUsesPeerDiscoveryByDefault(t *testing.T) {
	path := writeTempConfig(t, `
mode: client
token: token
group: 123
`)

	config, err := LoadRuntimeConfig(path)
	if err != nil {
		t.Fatalf("LoadRuntimeConfig() error = %v", err)
	}
	if len(config.PeerIDs) != 0 {
		t.Fatalf("peer count = %d, want 0 before discovery", len(config.PeerIDs))
	}
	if config.HandshakePhrase != DefaultHandshakePhrase {
		t.Fatalf("HandshakePhrase = %q, want %q", config.HandshakePhrase, DefaultHandshakePhrase)
	}
}

func TestLoadRuntimeConfigRequiresToken(t *testing.T) {
	path := writeTempConfig(t, `
mode: server
group: 123
`)

	if _, err := LoadRuntimeConfig(path); err == nil {
		t.Fatal("LoadRuntimeConfig() error = nil, want error")
	}
}

func TestLoadRuntimeConfigUsesCustomHandshakePhrase(t *testing.T) {
	path := writeTempConfig(t, `
mode: server
token: token
group: 123
handshake_phrase: "/overvk-prod"
`)

	config, err := LoadRuntimeConfig(path)
	if err != nil {
		t.Fatalf("LoadRuntimeConfig() error = %v", err)
	}
	if config.HandshakePhrase != "/overvk-prod" {
		t.Fatalf("HandshakePhrase = %q, want custom phrase", config.HandshakePhrase)
	}
}

func TestLoadRuntimeConfigAcceptsLegacyDiscoveryPhrase(t *testing.T) {
	path := writeTempConfig(t, `
mode: server
token: token
group: 123
discovery_phrase: "/overvk-legacy"
`)

	config, err := LoadRuntimeConfig(path)
	if err != nil {
		t.Fatalf("LoadRuntimeConfig() error = %v", err)
	}
	if config.HandshakePhrase != "/overvk-legacy" {
		t.Fatalf("HandshakePhrase = %q, want legacy phrase", config.HandshakePhrase)
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}
