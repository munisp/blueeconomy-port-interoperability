package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/munisp/blueeconomy-port-interoperability/internal/waste"
	"github.com/stretchr/testify/require"
)

func newUnreachablePool() (*pgxpool.Pool, error) {
	return pgxpool.New(context.Background(), "postgres://127.0.0.1:1/unreachable")
}

type fakeWasteStore struct {
	receipt waste.Receipt
	err     error
}

func (fake fakeWasteStore) Create(_ context.Context, _ string, request waste.CreateRequest, _ waste.Principal) (waste.Receipt, error) {
	if fake.err != nil {
		return waste.Receipt{}, fake.err
	}
	return waste.Receipt{ReceiptID: request.ReceiptID, PortCallID: request.PortCallID, Status: waste.StatusDraft}, nil
}
func (fake fakeWasteStore) MarkDelivered(_ context.Context, _, receiptID string, _ waste.Principal) (waste.Receipt, error) {
	if fake.err != nil {
		return waste.Receipt{}, fake.err
	}
	return waste.Receipt{ReceiptID: receiptID, Status: waste.StatusDelivered}, nil
}
func (fake fakeWasteStore) Verify(_ context.Context, _, receiptID string, _ waste.Principal) (waste.Receipt, error) {
	if fake.err != nil {
		return waste.Receipt{}, fake.err
	}
	return waste.Receipt{ReceiptID: receiptID, Status: waste.StatusVerified}, nil
}
func (fake fakeWasteStore) ListByPortCall(_ context.Context, portCallID string) ([]waste.Receipt, error) {
	if fake.err != nil {
		return nil, fake.err
	}
	return []waste.Receipt{}, nil
}

func TestCreateWasteReceiptRequiresRole(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/port-calls/call-1/waste-receipts", `{}`, RoleTrader))
	require.Equal(t, http.StatusForbidden, recorder.Code)
}

func TestCreateWasteReceiptDraft(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	body := `{"receiptId":"wr-1","marpolAnnex":"I","wasteType":"oil residues (bilge)","quantity":2.5,"unit":"M3","facilityReference":"PRF-LAGOS-01","receiptDocumentId":"ev-123"}`
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/port-calls/call-1/waste-receipts", body, RolePortOperatorAdmin))
	require.Equal(t, http.StatusCreated, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"portCallId":"call-1"`)
	require.Contains(t, recorder.Body.String(), `"status":"DRAFT"`)
}

func TestWasteReceiptVerifyRequiresNPARole(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/waste-receipts/wr-1/transitions", `{"target":"VERIFIED"}`, RolePortOperatorAdmin))
	require.Equal(t, http.StatusForbidden, recorder.Code)
}

func TestWasteReceiptDeliverAndVerifyLifecycle(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	delivered := httptest.NewRecorder()
	handler.ServeHTTP(delivered, authedRequest(t, http.MethodPost, "/v1/waste-receipts/wr-1/transitions", `{"target":"DELIVERED"}`, RolePortOperatorAdmin))
	require.Equal(t, http.StatusOK, delivered.Code)
	require.Contains(t, delivered.Body.String(), `"status":"DELIVERED"`)
	verified := httptest.NewRecorder()
	handler.ServeHTTP(verified, authedRequest(t, http.MethodPost, "/v1/waste-receipts/wr-1/transitions", `{"target":"VERIFIED"}`, RoleNPAOfficer))
	require.Equal(t, http.StatusOK, verified.Code)
	require.Contains(t, verified.Body.String(), `"status":"VERIFIED"`)
}

func TestWasteReceiptRejectsUnknownTarget(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodPost, "/v1/waste-receipts/wr-1/transitions", `{"target":"DRAFT"}`, RolePortOperatorAdmin))
	require.Equal(t, http.StatusBadRequest, recorder.Code)
}

func TestListWasteReceiptsOnPortCall(t *testing.T) {
	handler := registryTestHandler(t, fakeRegistry{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodGet, "/v1/port-calls/call-1/waste-receipts", ``, RolePortOperatorAdmin))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"receipts":[]`)
}

func TestWasteStoreErrorMapping(t *testing.T) {
	config := testConfig()
	config.Waste = fakeWasteStore{err: waste.ErrNotFound}
	pool, err := newUnreachablePool()
	require.NoError(t, err)
	config.Pool = pool
	handler, err := New(config)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, authedRequest(t, http.MethodGet, "/v1/port-calls/missing/waste-receipts", ``, RolePortOperatorAdmin))
	require.Equal(t, http.StatusNotFound, recorder.Code)
}
