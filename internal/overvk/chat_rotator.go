package overvk

import "sync"

type ChatRotator struct {
	mu      sync.Mutex
	peerIDs []int
	index   int
}

func NewChatRotator(peerIDs []int) *ChatRotator {
	copied := append([]int(nil), peerIDs...)
	return &ChatRotator{peerIDs: copied}
}

func (r *ChatRotator) Next() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	peerID := r.peerIDs[r.index]
	r.index = (r.index + 1) % len(r.peerIDs)
	return peerID
}

func (r *ChatRotator) All() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.peerIDs...)
}
