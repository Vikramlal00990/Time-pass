FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o server ./api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8000
CMD ["./server"]
