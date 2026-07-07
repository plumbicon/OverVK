package overvk

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestDataSenderHandlerPacketModeSendsEachReadImmediately(t *testing.T) {
	previousEngine := Engine()
	t.Cleanup(func() { ConfigureEngine(previousEngine) })

	engine := DefaultEngineConfig()
	engine.PacketMode = true
	engine.TextOnly = true
	engine.MaxChunkSize = 4
	engine.MaxTextMessagePayload = 4
	engine.TextMessageThreshold = 4
	engine.ChunkTimeout = 20 * time.Millisecond
	ConfigureEngine(engine)

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	queue := NewSenderQueue(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int, 1)
	go func() {
		done <- DataSenderHandler(ctx, "Test", "packet-mode-test", right, TargetServer, queue)
	}()

	writeAndExpect := func(payload string, sequence int) {
		t.Helper()
		writeDone := make(chan error, 1)
		go func() {
			_, err := left.Write([]byte(payload))
			writeDone <- err
		}()

		select {
		case item := <-queue.ch:
			queue.wg.Done()
			if item.Done != nil {
				item.Done <- nil
				close(item.Done)
			}
			if item.Sequence != sequence {
				t.Fatalf("Sequence = %d, want %d", item.Sequence, sequence)
			}
			if string(item.Data) != payload {
				t.Fatalf("Data = %q, want %q", string(item.Data), payload)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for packet")
		}

		select {
		case err := <-writeDone:
			if err != nil {
				t.Fatalf("write error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for write")
		}
	}

	writeAndExpect("abc", 0)
	writeAndExpect("de", 1)

	_ = left.Close()
	select {
	case sent := <-done:
		if sent != 2 {
			t.Fatalf("sent sequence count = %d, want 2", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after connection close")
	}
}

func TestDataSenderHandlerFlushesAtDocumentChunkSize(t *testing.T) {
	previousEngine := Engine()
	t.Cleanup(func() { ConfigureEngine(previousEngine) })

	engine := DefaultEngineConfig()
	engine.PacketMode = false
	engine.TextOnly = false
	engine.MaxChunkSize = 100
	engine.MaxTextMessagePayload = 4
	engine.TextMessageThreshold = 8
	engine.DocumentChunkSize = 16
	engine.ChunkTimeout = time.Second
	ConfigureEngine(engine)

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	queue := NewSenderQueue(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int, 1)
	go func() {
		done <- DataSenderHandler(ctx, "Test", "hybrid-mode-test", right, TargetServer, queue)
	}()

	writeDone := make(chan error, 1)
	go func() {
		_, err := left.Write([]byte("1234567890abcdef"))
		writeDone <- err
	}()

	select {
	case item := <-queue.ch:
		queue.wg.Done()
		if item.Done != nil {
			item.Done <- nil
			close(item.Done)
		}
		if item.Sequence != 0 {
			t.Fatalf("Sequence = %d, want 0", item.Sequence)
		}
		if string(item.Data) != "1234567890abcdef" {
			t.Fatalf("Data = %q, want document-sized chunk", string(item.Data))
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for threshold flush")
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for write")
	}

	_ = left.Close()
	select {
	case sent := <-done:
		if sent != 1 {
			t.Fatalf("sent sequence count = %d, want 1", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after connection close")
	}
}
