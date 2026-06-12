FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o aegian ./cmd/node

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/aegian .
ENTRYPOINT ["./aegian"]