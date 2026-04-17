FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/app ./fetcher/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /bin/app .

COPY fetcher/config.yaml ./fetcher/config.yaml

CMD ["./app"]
