FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
ENV GOTOOLCHAIN=auto
ENV GOFLAGS=-mod=mod
RUN go mod download
RUN go build -o server ./api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
