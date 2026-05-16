package token

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/apm1432/worker/config"
)

// ─── Errors ───────────────────────────────────────────────────────────────────

var (
	ErrTokenInvalid = errors.New("stream token is invalid")
	ErrTokenExpired = errors.New("stream token has expired")
	ErrWrongType    = errors.New("this token was not issued for a worker endpoint")
)

// ─── Payload — must match main server's StreamPayload exactly ────────────────
//
// Main server च्या token.New() ने हे encrypt करतो.
// Worker फक्त decrypt + validate करतो.

type streamPayload struct {
	MessageID    int64     `json:"mid"`
	ChannelID    int64     `json:"cid"`
	UserID       string    `json:"uid"`
	SessionToken string    `json:"sid"` // worker tokens मध्ये हे "" असतं
	ExpiresAt    time.Time `json:"exp"`
}

// VerifyResult — caller ला दिलेलं decrypted payload.
type VerifyResult struct {
	MessageID int
	ChannelID int64
	UserID    string
}

// Verify decrypts the worker token and validates:
//  1. AES-256-GCM decryption (using STREAM_SECRET)
//  2. Expiry check
//  3. Worker token check: SessionToken must be "" (main server sets it empty for worker tokens)
//
// Session / nonce / access checks are NOT done here —
// those were already handled by the main server's handleWatch.
func Verify(tokenStr string) (*VerifyResult, error) {
	key := deriveKey()

	raw, err := base64.RawURLEncoding.DecodeString(tokenStr)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	nonceSize := gcm.NonceSize()
	if len(raw) < nonceSize {
		return nil, ErrTokenInvalid
	}

	nonce, ciphertext := raw[:nonceSize], raw[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrTokenInvalid
	}

	var payload streamPayload
	if err = json.Unmarshal(plaintext, &payload); err != nil {
		return nil, ErrTokenInvalid
	}

	if time.Now().After(payload.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	// SessionToken != "" म्हणजे हे main stream token आहे (worker token नाही).
	// Security: main stream token ला worker endpoint वर reject करा.
	if payload.SessionToken != "" {
		return nil, ErrWrongType
	}

	return &VerifyResult{
		MessageID: int(payload.MessageID),
		ChannelID: payload.ChannelID,
		UserID:    payload.UserID,
	}, nil
}

// deriveKey SHA-256 hash of STREAM_SECRET → 32-byte AES-256 key.
// Main server च्या token package सारखीच logic.
func deriveKey() []byte {
	sum := sha256.Sum256([]byte(config.C.StreamSecret))
	return sum[:]
}
