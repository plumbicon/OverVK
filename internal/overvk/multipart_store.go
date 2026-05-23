package overvk

import (
	"fmt"
	"sync"
)

type MultipartStore struct {
	mu      sync.Mutex
	buffers map[string]map[int]map[int][]byte
}

func NewMultipartStore() *MultipartStore {
	return &MultipartStore{buffers: map[string]map[int]map[int][]byte{}}
}

func (s *MultipartStore) Add(sessionID string, sequence, partIndex, totalParts int, data []byte) ([]byte, bool, error) {
	if totalParts <= 0 || partIndex < 0 || partIndex >= totalParts {
		return nil, false, fmt.Errorf("invalid part index %d/%d", partIndex, totalParts)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	bySession, ok := s.buffers[sessionID]
	if !ok {
		bySession = map[int]map[int][]byte{}
		s.buffers[sessionID] = bySession
	}
	bySequence, ok := bySession[sequence]
	if !ok {
		bySequence = map[int][]byte{}
		bySession[sequence] = bySequence
	}

	bySequence[partIndex] = append([]byte(nil), data...)
	if len(bySequence) != totalParts {
		return nil, false, nil
	}

	var complete []byte
	for i := 0; i < totalParts; i++ {
		part, ok := bySequence[i]
		if !ok {
			return nil, false, nil
		}
		complete = append(complete, part...)
	}

	delete(bySession, sequence)
	if len(bySession) == 0 {
		delete(s.buffers, sessionID)
	}
	return complete, true, nil
}

func (s *MultipartStore) Cleanup(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.buffers, sessionID)
}
