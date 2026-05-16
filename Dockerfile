FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.21 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /app/worker -ldflags="-w -s" .

FROM scratch
COPY --from=builder /app/worker /app/worker

EXPOSE ${PORT}
ENTRYPOINT ["/app/worker"]
