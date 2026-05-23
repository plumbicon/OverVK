package overvk

import (
	"log"
	"sync"
	"time"
)

type SessionMetrics struct {
	mu sync.Mutex

	sessionID      string
	startTime      time.Time
	lastReportTime time.Time

	bytesSent       int64
	bytesReceived   int64
	packetsSent     int64
	packetsReceived int64

	textMessagesSent      int64
	documentsSent         int64
	multipartMessagesSent int64
	sendErrors            int64
	receiveErrors         int64

	latencies []time.Duration

	smallPackets  int64
	mediumPackets int64
	largePackets  int64
	hugePackets   int64
}

var (
	metricsMu sync.Mutex
	metrics   = map[string]*SessionMetrics{}
)

func GetSessionMetrics(sessionID string) *SessionMetrics {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	if metric, ok := metrics[sessionID]; ok {
		return metric
	}
	metric := &SessionMetrics{
		sessionID:      sessionID,
		startTime:      time.Now(),
		lastReportTime: time.Now(),
	}
	metrics[sessionID] = metric
	return metric
}

func CleanupSessionMetrics(sessionID string) {
	metricsMu.Lock()
	metric, ok := metrics[sessionID]
	if ok {
		delete(metrics, sessionID)
	}
	metricsMu.Unlock()

	if ok {
		metric.Report()
	}
}

func ReportGlobalStatistics() {
	if !Engine().EnableSessionStatistics {
		return
	}

	metricsMu.Lock()
	snapshot := make([]*SessionMetrics, 0, len(metrics))
	for _, metric := range metrics {
		snapshot = append(snapshot, metric)
	}
	metricsMu.Unlock()

	if len(snapshot) == 0 {
		return
	}

	var totalSent, totalReceived, totalSentPackets, totalReceivedPackets int64
	var totalText, totalDocs, totalErrors int64
	for _, metric := range snapshot {
		metric.mu.Lock()
		totalSent += metric.bytesSent
		totalReceived += metric.bytesReceived
		totalSentPackets += metric.packetsSent
		totalReceivedPackets += metric.packetsReceived
		totalText += metric.textMessagesSent
		totalDocs += metric.documentsSent
		totalErrors += metric.sendErrors + metric.receiveErrors
		metric.mu.Unlock()
	}

	log.Print("============================================================")
	log.Print("=== GLOBAL SESSION STATISTICS ===")
	log.Printf("Active sessions: %d", len(snapshot))
	log.Printf("Total sent: %.2f KB", float64(totalSent)/1024)
	log.Printf("Total received: %.2f KB", float64(totalReceived)/1024)
	log.Printf("Total packets: sent=%d, received=%d", totalSentPackets, totalReceivedPackets)
	log.Printf("Messages: text=%d, documents=%d", totalText, totalDocs)
	if totalErrors > 0 {
		log.Printf("Total errors: %d", totalErrors)
	}
	log.Print("============================================================")
}

func (m *SessionMetrics) RecordSend(dataSize int, isText bool, numParts int, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bytesSent += int64(dataSize)
	m.packetsSent++

	if !success {
		m.sendErrors++
		return
	}

	if isText {
		m.textMessagesSent += int64(numParts)
		if numParts > 1 {
			m.multipartMessagesSent++
		}
	} else {
		m.documentsSent++
	}

	switch {
	case dataSize < 1024:
		m.smallPackets++
	case dataSize < 10*1024:
		m.mediumPackets++
	case dataSize < 100*1024:
		m.largePackets++
	default:
		m.hugePackets++
	}
}

func (m *SessionMetrics) RecordReceive(dataSize int, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.bytesReceived += int64(dataSize)
	m.packetsReceived++
	if !success {
		m.receiveErrors++
	}
}

func (m *SessionMetrics) RecordLatency(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.latencies) >= 10 {
		copy(m.latencies, m.latencies[1:])
		m.latencies = m.latencies[:9]
	}
	m.latencies = append(m.latencies, latency)
}

func (m *SessionMetrics) ShouldReportPeriodic(interval time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if now.Sub(m.lastReportTime) >= interval {
		m.lastReportTime = now
		return true
	}
	return false
}

func (m *SessionMetrics) ReportPeriodic() {
	if !Engine().EnableSessionStatistics {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	duration := time.Since(m.startTime).Seconds()
	if duration <= 0 {
		duration = 1
	}
	log.Printf("[%s] Stats: Duration=%.1fs, Sent=%.1fKB(%.1fKB/s), Recv=%.1fKB(%.1fKB/s), Pkts=%d/%d, TxtMsg=%d, Docs=%d",
		m.sessionID,
		duration,
		float64(m.bytesSent)/1024,
		float64(m.bytesSent)/duration/1024,
		float64(m.bytesReceived)/1024,
		float64(m.bytesReceived)/duration/1024,
		m.packetsSent,
		m.packetsReceived,
		m.textMessagesSent,
		m.documentsSent,
	)
}

func (m *SessionMetrics) Report() {
	if !Engine().EnableSessionStatistics {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	duration := time.Since(m.startTime).Seconds()
	if duration <= 0 {
		duration = 1
	}

	log.Printf("[%s] === Session Statistics ===", m.sessionID)
	log.Printf("[%s] Duration: %.2fs", m.sessionID, duration)
	log.Printf("[%s] Sent: %.2f KB (%.2f KB/s)", m.sessionID, float64(m.bytesSent)/1024, float64(m.bytesSent)/duration/1024)
	log.Printf("[%s] Received: %.2f KB (%.2f KB/s)", m.sessionID, float64(m.bytesReceived)/1024, float64(m.bytesReceived)/duration/1024)
	log.Printf("[%s] Packets sent: %d, received: %d", m.sessionID, m.packetsSent, m.packetsReceived)
	log.Printf("[%s] Text messages: %d, Documents: %d, Multi-part: %d", m.sessionID, m.textMessagesSent, m.documentsSent, m.multipartMessagesSent)
	if len(m.latencies) > 0 {
		var total time.Duration
		for _, latency := range m.latencies {
			total += latency
		}
		log.Printf("[%s] Avg latency: %.2fms", m.sessionID, float64(total.Microseconds())/1000/float64(len(m.latencies)))
	}
	if m.sendErrors > 0 || m.receiveErrors > 0 {
		log.Printf("[%s] Errors: send=%d, receive=%d", m.sessionID, m.sendErrors, m.receiveErrors)
	}
	log.Printf("[%s] Packet size distribution: small(<1KB)=%d, medium(1-10KB)=%d, large(10-100KB)=%d, huge(>100KB)=%d",
		m.sessionID, m.smallPackets, m.mediumPackets, m.largePackets, m.hugePackets)
}
