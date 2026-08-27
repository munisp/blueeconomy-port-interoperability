package nswsecurity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type memoryReplayStore struct {
	mu   sync.Mutex
	seen map[string]bool
	down bool
}

func (store *memoryReplayStore) Reserve(_ context.Context, replayHash string, _ time.Time) (bool, error) {
	if store.down {
		return false, context.DeadlineExceeded
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.seen == nil {
		store.seen = map[string]bool{}
	}
	if store.seen[replayHash] {
		return false, nil
	}
	store.seen[replayHash] = true
	return true, nil
}

func newIngressFixture(t *testing.T) (http.Handler, *memoryReplayStore, authorityFixture) {
	t.Helper()
	fixture := newAuthorityFixture(t)
	verifier := fixture.verifier(t, map[string]bool{"RS256": true})
	replay := &memoryReplayStore{}
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := ClaimsFrom(r.Context())
		if err != nil {
			http.Error(w, "missing claims", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Verified-Tenant", claims.TenantID)
		w.Header().Set("X-Verified-JTI", claims.JTI)
		w.WriteHeader(http.StatusNoContent)
	})
	handler, err := NewIngress(IngressConfig{
		SignatureHeader: "X-NSW-Signature",
		Verifier:        verifier,
		ReplayStore:     replay,
		ReplayTTL:       time.Hour,
	}, downstream)
	if err != nil {
		t.Fatalf("build ingress: %v", err)
	}
	return handler, replay, fixture
}

func TestIngressPassesValidSignatureAndExposesClaims(t *testing.T) {
	handler, _, fixture := newIngressFixture(t)
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	request := httptest.NewRequest(http.MethodPost, "/v1/nsw/port-calls", nil)
	request.Header.Set("X-NSW-Signature", token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("valid authority message = %d, want 204", response.Code)
	}
	if response.Header().Get("X-Verified-Tenant") != "tenant-apapa-port" || response.Header().Get("X-Verified-JTI") != "nsw-msg-0001" {
		t.Fatalf("verified claims not propagated: %v", response.Header())
	}
}

func TestIngressRejectsMissingSignature(t *testing.T) {
	handler, _, _ := newIngressFixture(t)
	request := httptest.NewRequest(http.MethodPost, "/v1/nsw/port-calls", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing signature = %d, want 401", response.Code)
	}
}

func TestIngressRejectsReplay(t *testing.T) {
	handler, _, fixture := newIngressFixture(t)
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	for attempt, want := range []int{http.StatusNoContent, http.StatusConflict} {
		request := httptest.NewRequest(http.MethodPost, "/v1/nsw/port-calls", nil)
		request.Header.Set("X-NSW-Signature", token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("attempt %d = %d, want %d", attempt, response.Code, want)
		}
	}
}

func TestIngressFailsClosedWhenReplayStoreDown(t *testing.T) {
	handler, replay, fixture := newIngressFixture(t)
	replay.down = true
	now := time.Now().UTC()
	token := fixture.sign(t, map[string]any{"alg": "RS256", "kid": fixture.kid}, validClaims(now))
	request := httptest.NewRequest(http.MethodPost, "/v1/nsw/port-calls", nil)
	request.Header.Set("X-NSW-Signature", token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("replay store down = %d, want 503 (fail-closed)", response.Code)
	}
}

func TestNewIngressFailsClosedWithoutDependencies(t *testing.T) {
	if _, err := NewIngress(IngressConfig{}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); err == nil {
		t.Fatal("empty ingress config must fail closed")
	}
}
