package overvk

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync"
)

var (
	cipherMu     sync.RWMutex
	activeCipher cipher.AEAD
)

func ConfigureCipher(key string) error {
	if key == "" {
		cipherMu.Lock()
		activeCipher = nil
		cipherMu.Unlock()
		return nil
	}

	hash := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		return fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("create GCM: %w", err)
	}

	cipherMu.Lock()
	activeCipher = gcm
	cipherMu.Unlock()
	return nil
}

func Encrypt(plaintext []byte) ([]byte, error) {
	cipherMu.RLock()
	gcm := activeCipher
	cipherMu.RUnlock()

	if gcm == nil {
		return plaintext, nil
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func Decrypt(ciphertext []byte) ([]byte, error) {
	cipherMu.RLock()
	gcm := activeCipher
	cipherMu.RUnlock()

	if gcm == nil {
		return ciphertext, nil
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := ciphertext[:nonceSize]
	return gcm.Open(nil, nonce, ciphertext[nonceSize:], nil)
}

func CipherEnabled() bool {
	cipherMu.RLock()
	defer cipherMu.RUnlock()
	return activeCipher != nil
}

func EncryptMessageText(plaintext string) (string, error) {
	cipherMu.RLock()
	gcm := activeCipher
	cipherMu.RUnlock()

	if gcm == nil {
		return plaintext, nil
	}

	ciphertext, err := Encrypt([]byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func DecryptMessageText(encoded string) (string, error) {
	cipherMu.RLock()
	gcm := activeCipher
	cipherMu.RUnlock()

	if gcm == nil {
		return encoded, nil
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("base64 decode encrypted message: %w", err)
	}
	plaintext, err := Decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}
