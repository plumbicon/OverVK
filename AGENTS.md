# AGENTS.md

## Project Overview

OverVK is a Go application that creates a SOCKS5 tunnel over VK community messages. A single binary, `overvk`, runs in either `client` or `server` mode depending on the YAML config passed as the only command-line argument.

The client starts a local SOCKS5 proxy, accepts browser/application TCP connections, and sends tunnel control/data messages through VK. The server listens to VK Group Long Poll, opens outbound TCP connections to requested destinations, and sends responses back through VK message channels.

Client/server chat selection is automatic. The client discovers candidate VK chat peer IDs, sends `ready` handshakes, the server replies with `ready_ack` in every shared chat it sees, and both sides then rotate outgoing messages across all discovered shared chats. Configs should not require a manually supplied `peer_id`.

## Tech Stack

- Language: Go
- Runtime: single compiled Go binary
- Config format: YAML
- VK transport:
  - VK API method calls over HTTPS
  - VK Group Long Poll
  - text messages for small Base64-encoded chunks
  - VK documents for larger chunks
- Networking:
  - local SOCKS5 proxy on the client side
  - outbound TCP dialing on the server side
- External Go dependency:
  - `gopkg.in/yaml.v3`

## Repository Layout

- `cmd/overvk/main.go`: application entrypoint, mode selection, client/server runtime logic.
- `internal/overvk/config.go`: YAML config loading and validation.
- `internal/overvk/constants.go`: default engine settings.
- `internal/overvk/vk_api.go`: VK API and Group Long Poll helpers.
- `internal/overvk/protocol.go`: tunnel message protocol, chunk sending, document upload.
- `internal/overvk/stream.go`: stream reading, chunking, ordered packet writing.
- `internal/overvk/sender.go`: async sender queue and worker pool.
- `internal/overvk/chat_rotator.go`: cyclic peer selection across discovered shared chats.
- `client.config.example.yaml`: example client config.
- `server.config.example.yaml`: example server config.
- `README.md`: user-facing setup and run instructions.

## Build And Run

Build:

```bash
go build -o bin/overvk ./cmd/overvk
```

Run:

```bash
./bin/overvk path/to/config.yaml
```

Check:

```bash
go test ./...
go vet ./...
```

## Agent Instructions

- Preserve the single-binary model: `overvk <config.yaml>`.
- Keep client/server behavior selected by the YAML `mode` field.
- Preserve automatic multi-chat bootstrap; do not reintroduce a required manual `peer_id` config.
- Prefer standard-library Go unless a dependency is clearly justified.
- Keep secrets out of git; real VK tokens belong only in local config files.
- When changing VK API calls, Group Long Poll behavior, document upload flow, or any code that talks to an external API, the model must use Context7 to verify current API usage and examples before implementing or explaining the change.
- For OpenAI/API-related questions, also follow the project instruction to use official documentation sources.
