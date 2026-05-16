package tgutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/storage"
	"github.com/coocood/freecache"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

// ─── File metadata ────────────────────────────────────────────────────────────

type File struct {
	Location tg.InputFileLocationClass
	FileSize int64
	FileName string
	MimeType string
	ID       int64
}

// ─── In-memory cache (file metadata — 1 hour TTL) ────────────────────────────

var (
	fileCache     *freecache.Cache
	fileCacheOnce sync.Once
)

func initCache() *freecache.Cache {
	fileCacheOnce.Do(func() {
		fileCache = freecache.NewCache(32 * 1024 * 1024) // 32 MB
	})
	return fileCache
}

// ─── File fetch ───────────────────────────────────────────────────────────────

// FileFromMessageInChannel fetches file metadata from a Telegram channel message.
// Results are cached in memory for 1 hour to avoid repeated Telegram API calls.
func FileFromMessageInChannel(ctx context.Context, client *gotgproto.Client, messageID int, channelID int64, log *zap.Logger) (*File, error) {
	cache := initCache()
	cacheKey := []byte(fmt.Sprintf("file:%d:%d:%d", messageID, channelID, client.Self.ID))

	// Cache hit?
	if cached, err := cache.Get(cacheKey); err == nil {
		file, err := deserializeFile(cached)
		if err == nil {
			log.Debug("file metadata from cache",
				zap.Int("messageID", messageID),
				zap.Int64("channelID", channelID),
			)
			return file, nil
		}
	}

	log.Debug("fetching file metadata from Telegram",
		zap.Int("messageID", messageID),
		zap.Int64("channelID", channelID),
	)

	channel, err := GetChannelPeer(ctx, client.API(), client.PeerStorage, channelID)
	if err != nil {
		return nil, fmt.Errorf("GetChannelPeer: %w", err)
	}

	res, err := client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{
			ChannelID:  channel.ChannelID,
			AccessHash: channel.AccessHash,
		},
		ID: []tg.InputMessageClass{&tg.InputMessageID{ID: messageID}},
	})
	if err != nil {
		return nil, fmt.Errorf("ChannelsGetMessages: %w", err)
	}

	msgs, ok := res.(*tg.MessagesChannelMessages)
	if !ok || len(msgs.Messages) == 0 {
		return nil, fmt.Errorf("message %d not found in channel %d", messageID, channelID)
	}

	msg, ok := msgs.Messages[0].(*tg.Message)
	if !ok {
		return nil, fmt.Errorf("message %d was deleted", messageID)
	}

	file, err := fileFromMedia(msg.Media)
	if err != nil {
		return nil, err
	}

	// Cache for 1 hour
	if data, err := serializeFile(file); err == nil {
		_ = cache.Set(cacheKey, data, int(time.Hour.Seconds()))
	}

	return file, nil
}

// ─── Channel peer resolver ────────────────────────────────────────────────────

func GetChannelPeer(ctx context.Context, api *tg.Client, peerStorage *storage.PeerStorage, channelID int64) (*tg.InputChannel, error) {
	// -100 prefix strip करा MTProto साठी
	mtprotoID := channelID
	if mtprotoID < 0 {
		mtprotoID = (-mtprotoID) - 1000000000000
	}

	// Peer storage मधून try करा
	cachedPeer := peerStorage.GetInputPeerById(mtprotoID)
	switch peer := cachedPeer.(type) {
	case *tg.InputPeerChannel:
		return &tg.InputChannel{
			ChannelID:  peer.ChannelID,
			AccessHash: peer.AccessHash,
		}, nil
	}

	// Storage miss — Telegram API call
	channels, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: mtprotoID},
	})
	if err != nil {
		return nil, fmt.Errorf("ChannelsGetChannels: %w", err)
	}
	if len(channels.GetChats()) == 0 {
		return nil, errors.New("no channels found")
	}
	channel, ok := channels.GetChats()[0].(*tg.Channel)
	if !ok {
		return nil, errors.New("type assertion to *tg.Channel failed")
	}
	peerStorage.AddPeer(channel.GetID(), channel.AccessHash, storage.TypeChannel, "")
	return channel.AsInput(), nil
}

// ─── Media → File ─────────────────────────────────────────────────────────────

func fileFromMedia(media tg.MessageMediaClass) (*File, error) {
	switch m := media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := m.Document.AsNotEmpty()
		if !ok {
			return nil, fmt.Errorf("unexpected document type %T", m)
		}
		var fileName string
		for _, attr := range doc.Attributes {
			if a, ok := attr.(*tg.DocumentAttributeFilename); ok {
				fileName = a.FileName
				break
			}
		}
		return &File{
			Location: doc.AsInputDocumentFileLocation(),
			FileSize: doc.Size,
			FileName: fileName,
			MimeType: doc.MimeType,
			ID:       doc.ID,
		}, nil

	case *tg.MessageMediaPhoto:
		photo, ok := m.Photo.AsNotEmpty()
		if !ok {
			return nil, fmt.Errorf("unexpected photo type %T", m)
		}
		sizes := photo.Sizes
		if len(sizes) == 0 {
			return nil, errors.New("photo has no sizes")
		}
		photoSize, ok := sizes[len(sizes)-1].AsNotEmpty()
		if !ok {
			return nil, errors.New("photo size is empty")
		}
		return &File{
			Location: &tg.InputPhotoFileLocation{
				ID:            photo.GetID(),
				AccessHash:    photo.GetAccessHash(),
				FileReference: photo.GetFileReference(),
				ThumbSize:     photoSize.GetType(),
			},
			FileSize: 0, // caller photo check करतो (FileSize == 0)
			FileName: fmt.Sprintf("photo_%d.jpg", photo.GetID()),
			MimeType: "image/jpeg",
			ID:       photo.GetID(),
		}, nil
	}

	return nil, fmt.Errorf("unsupported media type %T", media)
}

// ─── Simple serialization for cache ──────────────────────────────────────────
// File मध्ये interface (tg.InputFileLocationClass) आहे — JSON serialize होत नाही.
// FileSize == 0 (photo) असेल तेव्हाच पुन्हा fetch करणं OK आहे.
// म्हणून simple approach: फक्त document files cache करतो.

func serializeFile(f *File) ([]byte, error) {
	if f.FileSize == 0 {
		return nil, errors.New("photo — don't cache location")
	}
	loc, ok := f.Location.(*tg.InputDocumentFileLocation)
	if !ok {
		return nil, errors.New("unsupported location type for caching")
	}
	data := fmt.Sprintf("%d|%d|%d|%s|%s|%s",
		loc.ID, loc.AccessHash, f.FileSize,
		f.FileName, f.MimeType,
		string(loc.FileReference),
	)
	return []byte(data), nil
}

func deserializeFile(data []byte) (*File, error) {
	var (
		locID, locHash, fileSize int64
		fileName, mimeType, ref  string
	)
	_, err := fmt.Sscanf(string(data), "%d|%d|%d|%s|%s|%s",
		&locID, &locHash, &fileSize, &fileName, &mimeType, &ref)
	if err != nil {
		return nil, err
	}
	return &File{
		Location: &tg.InputDocumentFileLocation{
			ID:            locID,
			AccessHash:    locHash,
			FileReference: []byte(ref),
		},
		FileSize: fileSize,
		FileName: fileName,
		MimeType: mimeType,
	}, nil
}

// ─── Client disconnect error check ───────────────────────────────────────────

func IsClientDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return contains(s, "connection was aborted") ||
		contains(s, "connection reset by peer") ||
		contains(s, "broken pipe") ||
		contains(s, "forcibly closed")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
