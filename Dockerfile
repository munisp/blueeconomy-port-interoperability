FROM golang:1.25-bookworm AS build
WORKDIR /src
# go.mod's replace github.com/munisp/blueeconomy-contracts/gen/go =>
# ../blueeconomy-contracts/gen/go needs that sibling module physically
# present one directory above /src at build time (same real gap found and
# fixed in blueeconomy-financial-controls's Dockerfile - neither repo's
# Dockerfile nor CI ever checked it out). Build context must be the parent
# directory containing both repos as siblings.
COPY blueeconomy-contracts/gen/go /blueeconomy-contracts/gen/go
COPY blueeconomy-port-interoperability/go.mod blueeconomy-port-interoperability/go.sum ./
RUN go mod download
COPY blueeconomy-port-interoperability/. .
# CGO is required: the TigerBeetle client links a native static library.
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/port-interoperability ./cmd/port-interoperability
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/ussd-gateway ./cmd/ussd-gateway
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/booking-worker ./cmd/booking-worker
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/outbox-publisher ./cmd/outbox-publisher

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/port-interoperability /port-interoperability
COPY --from=build /out/ussd-gateway /ussd-gateway
COPY --from=build /out/booking-worker /booking-worker
COPY --from=build /out/outbox-publisher /outbox-publisher
USER nonroot:nonroot
ENTRYPOINT ["/port-interoperability"]
