# Start from the official Golang image for building
FROM golang:1.24.2-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -o alldbtest main.go

# Start a new minimal image
FROM alpine:latest

WORKDIR /app

# Copy the built binary from builder
COPY --from=builder /app/alldbtest .

# Expose port (change if your app uses a different port)
EXPOSE 8000

# Run the binary
CMD ["./alldbtest"]
