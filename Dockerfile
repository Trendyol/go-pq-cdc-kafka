FROM golang:1.25-alpine AS builder

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -o main ./cmd/connector

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/main /app/main
COPY --from=builder /app/resources/config.yml /app/resources/config.yml
COPY --from=builder /app/config /app/config

EXPOSE 8080

ENTRYPOINT ["./main"]
