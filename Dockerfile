FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o inbox ./cmd

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /app/inbox .
COPY internal/migrations/ internal/migrations/
ENTRYPOINT ["/app/inbox"]
