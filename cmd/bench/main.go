package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

type params struct {
	ChunkTimeout         time.Duration
	TextMessageThreshold int
}

type result struct {
	URL        string
	Status     int
	TTFB       time.Duration
	Total      time.Duration
	Bytes      int64
	Throughput float64
	Err        error
}

type comboResult struct {
	Params          params
	MedianTTFB      time.Duration
	MedianTotal     time.Duration
	AvgThroughput   float64
	TotalBytes      int64
	SuccessRate     float64
	Results         []result
}

func main() {
	socksPort := flag.Int("socks-port", 8888, "SOCKS5 port for test client")
	iterations := flag.Int("n", 5, "iterations per URL per combo")
	urlList := flag.String("urls", "", "comma-separated URLs (default: built-in)")
	binaryPath := flag.String("bin", "", "path to overvk binary (default: auto-build)")
	serverCfg := flag.String("server-config", "config/server.yaml", "base server config")
	clientCfg := flag.String("client-config", "config/client.yaml", "base client config")
	requestTimeout := flag.Duration("timeout", 120*time.Second, "per-request timeout")
	bootstrapWait := flag.Duration("bootstrap", 15*time.Second, "time to wait for bootstrap")
	flag.Parse()

	urls := defaultURLs()
	if *urlList != "" {
		urls = strings.Split(*urlList, ",")
		for i := range urls {
			urls[i] = strings.TrimSpace(urls[i])
		}
	}

	bin := *binaryPath
	if bin == "" {
		fmt.Println("Building overvk...")
		bin = filepath.Join(os.TempDir(), "overvk-bench")
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/overvk")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Build failed: %v\n", err)
			os.Exit(1)
		}
	}

	serverBase, err := os.ReadFile(*serverCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot read server config: %v\n", err)
		os.Exit(1)
	}
	clientBase, err := os.ReadFile(*clientCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot read client config: %v\n", err)
		os.Exit(1)
	}

	var combos []params
	for threshold := 8 * 1024; threshold <= 10*1024; threshold += 1024 {
		for chunkMs := 10; chunkMs <= 40; chunkMs += 10 {
			combos = append(combos, params{
				ChunkTimeout:         time.Duration(chunkMs) * time.Millisecond,
				TextMessageThreshold: threshold,
			})
		}
	}

	fmt.Printf("OverVK Parameter Sweep\n")
	fmt.Printf("Combos: %d | URLs: %d | Iterations: %d\n", len(combos), len(urls), *iterations)
	fmt.Printf("Threshold: 8KB-10KB (step 1KB) | ChunkTimeout: 10ms-40ms (step 10ms)\n")
	fmt.Println(strings.Repeat("═", 110))

	var allCombos []comboResult

	for ci, combo := range combos {
		fmt.Printf("\n[%d/%d] threshold=%dKB chunk_timeout=%dms\n",
			ci+1, len(combos), combo.TextMessageThreshold/1024, combo.ChunkTimeout.Milliseconds())

		cr := runCombo(bin, string(serverBase), string(clientBase), combo, urls,
			*socksPort, *iterations, *requestTimeout, *bootstrapWait)
		allCombos = append(allCombos, cr)

		if len(cr.Results) == 0 {
			fmt.Printf("  SKIP: no successful results\n")
			continue
		}
		fmt.Printf("  TTFB median=%dms | Total median=%dms | Throughput=%.1f KB/s | %.1fKB total | Success=%.0f%%\n",
			cr.MedianTTFB.Milliseconds(),
			cr.MedianTotal.Milliseconds(),
			cr.AvgThroughput,
			float64(cr.TotalBytes)/1024,
			cr.SuccessRate*100)
	}

	fmt.Println()
	fmt.Println(strings.Repeat("═", 110))
	printLeaderboard(allCombos)
}

func runCombo(bin, serverBase, clientBase string, p params, urls []string,
	socksPort, iterations int, requestTimeout, bootstrapWait time.Duration) comboResult {

	tmpDir, err := os.MkdirTemp("", "overvk-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "  tmpdir error: %v\n", err)
		return comboResult{Params: p}
	}
	defer os.RemoveAll(tmpDir)

	engineOverride := fmt.Sprintf("\n  chunk_timeout: %s\n  text_message_threshold: %d\n",
		p.ChunkTimeout.String(), p.TextMessageThreshold)

	serverYAML := injectEngineParams(serverBase, engineOverride)
	clientYAML := injectEngineParams(clientBase, engineOverride)

	serverFile := filepath.Join(tmpDir, "server.yaml")
	clientFile := filepath.Join(tmpDir, "client.yaml")
	os.WriteFile(serverFile, []byte(serverYAML), 0644)
	os.WriteFile(clientFile, []byte(clientYAML), 0644)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCmd := exec.CommandContext(ctx, bin, serverFile)
	serverCmd.Stdout = io.Discard
	serverCmd.Stderr = io.Discard
	if err := serverCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "  server start error: %v\n", err)
		return comboResult{Params: p}
	}
	defer func() {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}()

	time.Sleep(1 * time.Second)

	clientCmd := exec.CommandContext(ctx, bin, clientFile)
	clientCmd.Stdout = io.Discard
	clientCmd.Stderr = io.Discard
	if err := clientCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "  client start error: %v\n", err)
		return comboResult{Params: p}
	}
	defer func() {
		clientCmd.Process.Kill()
		clientCmd.Wait()
	}()

	if !waitForSOCKS(fmt.Sprintf("127.0.0.1:%d", socksPort), bootstrapWait) {
		fmt.Fprintf(os.Stderr, "  SOCKS5 not ready after %s\n", bootstrapWait)
		return comboResult{Params: p}
	}

	time.Sleep(2 * time.Second)

	dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort), nil, proxy.Direct)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  proxy error: %v\n", err)
		return comboResult{Params: p}
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: false},
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: transport, Timeout: requestTimeout}

	var results []result
	for _, u := range urls {
		for i := 0; i < iterations; i++ {
			r := fetch(client, u)
			results = append(results, r)
		}
	}

	transport.CloseIdleConnections()
	cancel()

	return summarizeCombo(p, results)
}

func injectEngineParams(base, override string) string {
	lines := strings.Split(base, "\n")
	var out []string
	skipIndented := false
	engineFound := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if trimmed == "engine:" {
			engineFound = true
			skipIndented = true
			out = append(out, "engine:"+override)
			continue
		}

		if skipIndented {
			if strings.HasPrefix(line, "  ") && !strings.HasPrefix(trimmed, "#") && trimmed != "" {
				continue
			}
			skipIndented = false
		}

		out = append(out, line)
	}

	if !engineFound {
		out = append(out, "engine:"+override)
	}

	return strings.Join(out, "\n")
}

func waitForSOCKS(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func fetch(client *http.Client, url string) result {
	start := time.Now()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return result{URL: url, Err: err}
	}
	req.Header.Set("User-Agent", "OverVK-Bench/1.0")

	resp, err := client.Do(req)
	ttfb := time.Since(start)
	if err != nil {
		return result{URL: url, TTFB: ttfb, Err: err}
	}
	defer resp.Body.Close()

	n, _ := io.Copy(io.Discard, resp.Body)
	total := time.Since(start)

	throughput := 0.0
	if total.Seconds() > 0 {
		throughput = float64(n) / 1024 / total.Seconds()
	}

	return result{
		URL: url, Status: resp.StatusCode,
		TTFB: ttfb, Total: total,
		Bytes: n, Throughput: throughput,
	}
}

func summarizeCombo(p params, results []result) comboResult {
	cr := comboResult{Params: p, Results: results}

	var ttfbs, totals []time.Duration
	var totalBytes int64
	var success int

	for _, r := range results {
		if r.Err != nil {
			continue
		}
		success++
		ttfbs = append(ttfbs, r.TTFB)
		totals = append(totals, r.Total)
		totalBytes += r.Bytes
	}

	if success == 0 {
		return cr
	}

	cr.SuccessRate = float64(success) / float64(len(results))
	cr.TotalBytes = totalBytes

	sort.Slice(ttfbs, func(i, j int) bool { return ttfbs[i] < ttfbs[j] })
	sort.Slice(totals, func(i, j int) bool { return totals[i] < totals[j] })

	cr.MedianTTFB = ttfbs[len(ttfbs)/2]
	cr.MedianTotal = totals[len(totals)/2]

	var totalTime time.Duration
	for _, t := range totals {
		totalTime += t
	}
	cr.AvgThroughput = float64(totalBytes) / 1024 / totalTime.Seconds()

	return cr
}

func printLeaderboard(combos []comboResult) {
	var valid []comboResult
	for _, c := range combos {
		if c.SuccessRate > 0.5 {
			valid = append(valid, c)
		}
	}

	if len(valid) == 0 {
		fmt.Println("No valid results to rank.")
		return
	}

	fmt.Println("TOP 10 BY MEDIAN TOTAL TIME (lower is better)")
	fmt.Println(strings.Repeat("─", 110))
	fmt.Printf("%-4s  %-12s  %-14s  %-12s  %-12s  %-12s  %-8s\n",
		"Rank", "Threshold", "ChunkTimeout", "Med TTFB", "Med Total", "Throughput", "Success")
	fmt.Println(strings.Repeat("─", 110))

	sort.Slice(valid, func(i, j int) bool { return valid[i].MedianTotal < valid[j].MedianTotal })

	top := 10
	if len(valid) < top {
		top = len(valid)
	}

	for i := 0; i < top; i++ {
		c := valid[i]
		fmt.Printf("#%-3d  %-12s  %-14s  %-12s  %-12s  %-12s  %-8s\n",
			i+1,
			fmt.Sprintf("%dKB", c.Params.TextMessageThreshold/1024),
			fmt.Sprintf("%dms", c.Params.ChunkTimeout.Milliseconds()),
			fmt.Sprintf("%dms", c.MedianTTFB.Milliseconds()),
			fmt.Sprintf("%dms", c.MedianTotal.Milliseconds()),
			fmt.Sprintf("%.1fKB/s", c.AvgThroughput),
			fmt.Sprintf("%.0f%%", c.SuccessRate*100))
	}

	fmt.Println()
	fmt.Println("TOP 10 BY THROUGHPUT (higher is better)")
	fmt.Println(strings.Repeat("─", 110))
	fmt.Printf("%-4s  %-12s  %-14s  %-12s  %-12s  %-12s  %-8s\n",
		"Rank", "Threshold", "ChunkTimeout", "Med TTFB", "Med Total", "Throughput", "Success")
	fmt.Println(strings.Repeat("─", 110))

	sort.Slice(valid, func(i, j int) bool { return valid[i].AvgThroughput > valid[j].AvgThroughput })

	for i := 0; i < top; i++ {
		c := valid[i]
		fmt.Printf("#%-3d  %-12s  %-14s  %-12s  %-12s  %-12s  %-8s\n",
			i+1,
			fmt.Sprintf("%dKB", c.Params.TextMessageThreshold/1024),
			fmt.Sprintf("%dms", c.Params.ChunkTimeout.Milliseconds()),
			fmt.Sprintf("%dms", c.MedianTTFB.Milliseconds()),
			fmt.Sprintf("%dms", c.MedianTotal.Milliseconds()),
			fmt.Sprintf("%.1fKB/s", c.AvgThroughput),
			fmt.Sprintf("%.0f%%", c.SuccessRate*100))
	}
}

func defaultURLs() []string {
	return []string{
		"http://example.com",
		"https://ifconfig.me",
		"https://www.google.com/robots.txt",
		"https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js",
		"https://norvig.com/big.txt",
	}
}
