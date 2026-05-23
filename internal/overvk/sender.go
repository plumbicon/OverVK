package overvk

import (
	"context"
	"log"
	"net/http"
	"sync"
)

type SenderQueue struct {
	ch chan SendItem
	wg sync.WaitGroup
}

func NewSenderQueue(capacity int) *SenderQueue {
	return &SenderQueue{ch: make(chan SendItem, capacity)}
}

func (q *SenderQueue) Enqueue(ctx context.Context, item SendItem) error {
	q.wg.Add(1)
	select {
	case q.ch <- item:
		return nil
	case <-ctx.Done():
		q.wg.Done()
		return ctx.Err()
	}
}

func (q *SenderQueue) Wait() {
	q.wg.Wait()
}

func StartSenderWorkers(ctx context.Context, queue *SenderQueue, client *http.Client, accessToken string, rotator *ChatRotator, workers int) []chan struct{} {
	doneSignals := make([]chan struct{}, 0, workers)
	for workerID := 0; workerID < workers; workerID++ {
		done := make(chan struct{})
		doneSignals = append(doneSignals, done)
		go func(workerID int, done chan struct{}) {
			defer close(done)
			log.Printf("[Worker-%d] Sender worker started", workerID)
			for {
				select {
				case <-ctx.Done():
					log.Printf("[Worker-%d] Sender worker stopped", workerID)
					return
				case item := <-queue.ch:
					func() {
						defer queue.wg.Done()
						peerID := rotator.Next()
						log.Printf("[Worker-%d][%s] Sending %d bytes for seq %d to peer %d", workerID, item.SessionID, len(item.Data), item.Sequence, peerID)
						if err := UploadAndSendChunk(ctx, client, accessToken, peerID, item.Target, item.SessionID, item.Sequence, item.Data); err != nil {
							log.Printf("[Worker-%d][%s] Sender failed for seq %d: %v", workerID, item.SessionID, item.Sequence, err)
						}
					}()
				}
			}
		}(workerID, done)
	}
	return doneSignals
}

func StopSenderWorkers(cancel context.CancelFunc, doneSignals []chan struct{}) {
	cancel()
	for _, done := range doneSignals {
		<-done
	}
}
