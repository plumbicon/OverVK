package overvk

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"time"
)

func DataSenderHandler(ctx context.Context, handlerName, sessionID string, conn net.Conn, target Target, senderQueue *SenderQueue) int {
	log.Printf("[%s] Starting %s handler", sessionID, handlerName)
	metric := GetSessionMetrics(sessionID)
	engine := Engine()

	buffer := make([]byte, 0, engine.MaxChunkSize)
	readBuffer := make([]byte, 32*1024)
	sequence := 0

	defer func() {
		log.Printf("[%s] %s handler stopped", sessionID, handlerName)
		CleanupSessionMetrics(sessionID)
	}()

	for {
		if metric.ShouldReportPeriodic(30 * time.Second) {
			metric.ReportPeriodic()
		}

		if err := conn.SetReadDeadline(time.Now().Add(engine.ChunkTimeout)); err != nil {
			log.Printf("[%s] failed to set read deadline: %v", sessionID, err)
		}

		n, err := conn.Read(readBuffer)
		if n > 0 {
			buffer = append(buffer, readBuffer[:n]...)
			for len(buffer) >= engine.MaxChunkSize {
				chunk := append([]byte(nil), buffer[:engine.MaxChunkSize]...)
				log.Printf("[%s] Queuing chunk of %d bytes (buffer full)", sessionID, len(chunk))
				if enqueueErr := senderQueue.Enqueue(ctx, SendItem{SessionID: sessionID, Sequence: sequence, Data: chunk, Target: target}); enqueueErr != nil {
					return sequence
				}
				buffer = buffer[engine.MaxChunkSize:]
				sequence++
			}
		}

		if err == nil {
			continue
		}

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			if len(buffer) > 0 {
				chunk := append([]byte(nil), buffer...)
				log.Printf("[%s] Queuing chunk of %d bytes due to timeout", sessionID, len(chunk))
				if enqueueErr := senderQueue.Enqueue(ctx, SendItem{SessionID: sessionID, Sequence: sequence, Data: chunk, Target: target}); enqueueErr != nil {
					return sequence
				}
				buffer = buffer[:0]
				sequence++
			}
			if ctx.Err() != nil {
				return sequence
			}
			continue
		}

		if errors.Is(err, io.EOF) || ctx.Err() != nil {
			if len(buffer) > 0 {
				chunk := append([]byte(nil), buffer...)
				log.Printf("[%s] Queuing remaining %d bytes at EOF", sessionID, len(chunk))
				_ = senderQueue.Enqueue(ctx, SendItem{SessionID: sessionID, Sequence: sequence, Data: chunk, Target: target})
				sequence++
			}
			return sequence
		}

		log.Printf("[%s] Unexpected error in %s: %v", sessionID, handlerName, err)
		return sequence
	}
}

func WriteOrderedPackets(ctx context.Context, sessionID string, conn net.Conn, incoming <-chan Packet, verbose bool) {
	defer conn.Close()

	nextExpected := 0
	packetBuffer := map[int][]byte{}
	bufferTimestamps := map[int]time.Time{}
	engine := Engine()

	for {
		for {
			payload, ok := packetBuffer[nextExpected]
			if !ok {
				break
			}
			delete(packetBuffer, nextExpected)
			delete(bufferTimestamps, nextExpected)
			if _, err := conn.Write(payload); err != nil {
				log.Printf("[%s] Failed to write buffered packet %d: %v", sessionID, nextExpected, err)
				return
			}
			if verbose {
				log.Printf("[%s] Wrote buffered packet %d", sessionID, nextExpected)
			}
			nextExpected++
		}

		now := time.Now()
		for sequence, timestamp := range bufferTimestamps {
			if now.Sub(timestamp) > engine.PacketBufferTimeout {
				delete(packetBuffer, sequence)
				delete(bufferTimestamps, sequence)
				log.Printf("[%s] Packet %d timeout, dropped from buffer", sessionID, sequence)
			}
		}

		select {
		case <-ctx.Done():
			return
		case packet, ok := <-incoming:
			if !ok || packet.Close {
				return
			}

			switch {
			case packet.Sequence == nextExpected:
				if _, err := conn.Write(packet.Data); err != nil {
					log.Printf("[%s] Failed to write packet %d: %v", sessionID, packet.Sequence, err)
					return
				}
				if verbose {
					log.Printf("[%s] Wrote packet %d", sessionID, packet.Sequence)
				}
				nextExpected++
			case packet.Sequence > nextExpected:
				if len(packetBuffer) >= engine.MaxPacketBufferSize {
					oldestSeq := -1
					var oldestTime time.Time
					for seq, ts := range bufferTimestamps {
						if oldestSeq == -1 || ts.Before(oldestTime) {
							oldestSeq = seq
							oldestTime = ts
						}
					}
					if oldestSeq >= 0 {
						delete(packetBuffer, oldestSeq)
						delete(bufferTimestamps, oldestSeq)
						log.Printf("[%s] Buffer overflow, dropped packet %d", sessionID, oldestSeq)
					}
				}
				packetBuffer[packet.Sequence] = append([]byte(nil), packet.Data...)
				bufferTimestamps[packet.Sequence] = time.Now()
				if verbose {
					log.Printf("[%s] Buffered out-of-order packet %d (expected %d)", sessionID, packet.Sequence, nextExpected)
				}
			default:
				if verbose {
					log.Printf("[%s] Discarding old packet %d (expected %d)", sessionID, packet.Sequence, nextExpected)
				}
			}
		}
	}
}
