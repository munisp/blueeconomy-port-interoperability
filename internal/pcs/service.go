// Package pcs aggregates the Port Community System integrations: AIS
// vessel-tracking ingestion, the TOS berth/operations adapter and the
// AIS↔TOS↔NSW port-call linkage. The whole surface is config-gated and
// fail-closed: without AIS_FEED_URL / TOS_ENDPOINT the corresponding legs
// report configured:false and every data path answers 503 honestly.
package pcs

import (
	"context"

	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/ais"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/linkage"
	"github.com/munisp/blueeconomy-port-interoperability/internal/pcs/tos"
)

// Service is the PCS aggregate consumed by the HTTP handlers. AIS, TOS
// and Linker may be nil individually; the status surface reports each
// leg's configuration honestly.
type Service struct {
	AIS    *ais.Ingester
	TOS    *tos.Client
	Linker *linkage.Linker
}

// TOSStatus is the honest TOS adapter status.
type TOSStatus struct {
	Configured bool   `json:"configured"`
	Circuit    string `json:"circuit"`
}

// AISStatus returns the ingestion status; configured:false when the feed
// is not wired.
func (service *Service) AISStatus() ais.Stats {
	if service == nil || service.AIS == nil {
		return ais.Stats{Configured: false}
	}
	return service.AIS.Stats()
}

// TOSStatus returns the adapter status; configured:false when the TOS
// endpoint is not wired.
func (service *Service) TOSStatus() TOSStatus {
	if service == nil || service.TOS == nil {
		return TOSStatus{Configured: false, Circuit: "unconfigured"}
	}
	return TOSStatus{Configured: true, Circuit: service.TOS.CircuitState()}
}

// LinkageConfigured reports which join legs are wired.
func (service *Service) LinkageConfigured() map[string]bool {
	if service == nil || service.Linker == nil {
		return map[string]bool{linkage.SourceNSW: false, linkage.SourceAIS: false, linkage.SourceTOS: false}
	}
	return service.Linker.Configured()
}

// LinkByIMO delegates the AIS↔TOS↔NSW join, failing closed when no
// linkage is wired.
func (service *Service) LinkByIMO(ctx context.Context, imo string, limit int) ([]linkage.LinkedPortCall, error) {
	if service == nil || service.Linker == nil {
		return nil, linkage.ErrUnconfigured
	}
	return service.Linker.LinkByIMO(ctx, imo, limit)
}

// ResolveMMSI delegates MMSI→IMO resolution, failing closed when no
// linkage is wired.
func (service *Service) ResolveMMSI(ctx context.Context, mmsi string) (string, bool) {
	if service == nil || service.Linker == nil {
		return "", false
	}
	return service.Linker.ResolveMMSI(ctx, mmsi)
}
