FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
ENV GOTOOLCHAIN=go1.21.13
RUN go env -w GOTOOLCHAIN=go1.21.13
RUN go build -o server ./api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
