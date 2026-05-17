package botpool

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/sessionMaker"
	"github.com/glebarez/sqlite"
	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/td/telegram"
	"go.uber.org/zap"
)

// clientKey uniquely identifies a bot by its token
type clientKey string

// entry holds a gotgproto client and its last-used time
type entry struct {
	client   *gotgproto.Client
	lastUsed time.Time
}

// Pool manages a cache of gotgproto clients keyed by bot token.
// Main server वरून येणाऱ्या token मध्ये bot credentials असतात —
// त्याच credentials वापरून client बनवतो/reuse करतो.
type Pool struct {
	mu      sync.Mutex
	clients map[clientKey]*entry
	log     *zap.Logger
}

var Default = &Pool{
	clients: make(map[clientKey]*entry),
}

// Init starts the background cleanup goroutine.
func Init(log *zap.Logger) {
	Default.log = log.Named("BotPool")
	go Default.cleanupLoop()
}

// Get returns an existing client for this bot token, or creates a new one.
func Get(apiID int32, apiHash, botToken string) (*gotgproto.Client, error) {
	return Default.Get(apiID, apiHash, botToken)
}

func (p *Pool) Get(apiID int32, apiHash, botToken string) (*gotgproto.Client, error) {
	key := clientKey(botToken)

	p.mu.Lock()
	if e, ok := p.clients[key]; ok {
		e.lastUsed = time.Now()
		p.mu.Unlock()
		return e.client, nil
	}
	p.mu.Unlock()

	// Client बनवतो — lock बाहेर (slow operation)
	client, err := p.start(apiID, apiHash, botToken)
	if err != nil {
		return nil, fmt.Errorf("botpool.start: %w", err)
	}

	p.mu.Lock()
	// Double-check — दुसऱ्या goroutine ने आधीच add केलं असेल तर
	if e, ok := p.clients[key]; ok {
		p.mu.Unlock()
		return e.client, nil
	}
	p.clients[key] = &entry{client: client, lastUsed: time.Now()}
	p.mu.Unlock()

	if p.log != nil {
		p.log.Sugar().Infof("Bot @%s connected", client.Self.Username)
	}
	return client, nil
}

// Count returns the number of active cached clients.
func Count() int {
	Default.mu.Lock()
	defer Default.mu.Unlock()
	return len(Default.clients)
}

// ─── Internal ─────────────────────────────────────────────────────────────────

func (p *Pool) start(apiID int32, apiHash, botToken string) (*gotgproto.Client, error) {
	// Session file: sessions/<first 8 chars of token>.session
	// Token चे पहिले 8 chars bot ID असतात — unique enough
	sessionName := "sessions/" + safeTokenPrefix(botToken)
	sessionPath := filepath.Clean(sessionName + ".session")

	return gotgproto.NewClient(
		int(apiID),
		apiHash,
		gotgproto.ClientTypeBot(botToken),
		&gotgproto.ClientOpts{
			Session:          sessionMaker.SqlSession(sqlite.Open(sessionPath)),
			DisableCopyright: true,
			Middlewares:      floodMiddleware(),
		},
	)
}

// cleanupLoop evicts clients unused for > 30 minutes.
func (p *Pool) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		p.evictStale(30 * time.Minute)
	}
}

func (p *Pool) evictStale(maxIdle time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.clients {
		if now.Sub(e.lastUsed) > maxIdle {
			delete(p.clients, k)
			if p.log != nil {
				p.log.Info("evicted idle bot client")
			}
		}
	}
}

func floodMiddleware() []telegram.Middleware {
	waiter := floodwait.NewSimpleWaiter().WithMaxWait(10 * time.Minute)
	return []telegram.Middleware{waiter}
}

// safeTokenPrefix extracts numeric bot ID from token (format: "12345678:AAA...")
func safeTokenPrefix(token string) string {
	for i, c := range token {
		if c == ':' {
			return token[:i]
		}
	}
	if len(token) > 8 {
		return token[:8]
	}
	return token
}

// Warmup pre-connects a bot from a context (used at startup for health verification)
func Warmup(ctx context.Context, apiID int32, apiHash, botToken string) error {
	_, err := Default.Get(apiID, apiHash, botToken)
	return err
}
