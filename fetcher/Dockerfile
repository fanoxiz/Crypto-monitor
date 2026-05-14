FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/app ./fetcher/main.go


FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /bin/app .

COPY fetcher/config.yaml ./fetcher/config.yaml

CMD ["./app"]
