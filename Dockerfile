FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/port-interoperability ./cmd/port-interoperability

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/port-interoperability /port-interoperability
USER nonroot:nonroot
ENTRYPOINT ["/port-interoperability"]
