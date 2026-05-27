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

// ─── Payload ──────────────────────────────────────────────────────────────────
// Main server च्या WorkerTokenPayload शी exactly match असणं आवश्यक.
// Main server हे encrypt करतो, worker फक्त decrypt + use करतो.

type WorkerTokenPayload struct {
	// Identity
	MessageID int64  `json:"mid"`
	ChannelID int64  `json:"cid"`
	UserID    string `json:"uid"`

	// Bot credentials — main server च्या bot pool मधून येतात
	// Worker स्वतः Telegram connect करतो याच credentials ने
	ApiID    int32  `json:"api_id"`
	ApiHash  string `json:"api_hash"`
	BotToken string `json:"bot_token"`

	// File location — main server ने आधीच Telegram ला query केलेली
	// Worker ला Telegram ला परत विचारायची गरज नाही
	FileType      string `json:"ftype"`  // "document" | "photo"
	FileID        int64  `json:"fid"`
	AccessHash    int64  `json:"fah"`
	FileReference []byte `json:"fref"`
	ThumbSize     string `json:"fthumb"` // फक्त photo साठी
	FileSize      int64  `json:"fsz"`
	FileName      string `json:"fname"`
	MimeType      string `json:"fmime"`
	DCID          int    `json:"dc"`

	// Stream identity key — main server प्रत्येक stream साठी unique key generate करतो.
	// Worker दर 5s ला /api/worker/stream-check ला हा key पाठवतो.
	// User ने नवीन stream सुरू केला की main server नवीन key store करतो —
	// जुना key invalid होतो → worker stream बंद करतो.
	StreamKey string `json:"sk"`

	// Expiry
	ExpiresAt time.Time `json:"exp"`
}

// VerifyResult — handler ला दिलेलं decrypted, validated payload
type VerifyResult struct {
	MessageID int64
	ChannelID int64
	UserID    string

	ApiID    int32
	ApiHash  string
	BotToken string

	FileType      string
	FileID        int64
	AccessHash    int64
	FileReference []byte
	ThumbSize     string
	FileSize      int64
	FileName      string
	MimeType      string
	DCID          int

	// Unique key for this stream session — used for heartbeat validation
	StreamKey string
}

// Verify decrypts and validates a worker token.
// Checks: AES-GCM decryption, expiry, worker-type marker (SessionToken must be "").
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

	var payload WorkerTokenPayload
	if err = json.Unmarshal(plaintext, &payload); err != nil {
		return nil, ErrTokenInvalid
	}

	if time.Now().After(payload.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	// BotToken empty म्हणजे हे worker token नाही
	if payload.BotToken == "" {
		return nil, ErrWrongType
	}

	return &VerifyResult{
		MessageID:     payload.MessageID,
		ChannelID:     payload.ChannelID,
		UserID:        payload.UserID,
		ApiID:         payload.ApiID,
		ApiHash:       payload.ApiHash,
		BotToken:      payload.BotToken,
		FileType:      payload.FileType,
		FileID:        payload.FileID,
		AccessHash:    payload.AccessHash,
		FileReference: payload.FileReference,
		ThumbSize:     payload.ThumbSize,
		FileSize:      payload.FileSize,
		FileName:      payload.FileName,
		MimeType:      payload.MimeType,
		DCID:          payload.DCID,
		StreamKey:     payload.StreamKey,
	}, nil
}

// deriveKey — STREAM_SECRET → 32-byte AES-256 key (main server सारखंच)
func deriveKey() []byte {
	sum := sha256.Sum256([]byte(config.C.StreamSecret))
	return sum[:]
}
