FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/app ./analyzer/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /bin/app .

COPY analyzer/config.yaml ./analyzer/config.yaml

CMD ["./app"]
