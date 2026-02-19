FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /build


COPY go.mod go.sum ./

RUN go mod download

COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o storemy .

# Stage 2: Runtime
FROM alpine:latest


RUN apk --no-cache add ca-certificates bash

RUN addgroup -g 1000 storemy && \
    adduser -D -u 1000 -G storemy storemy


WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/storemy .

# Copy sample SQL files
COPY --chown=storemy:storemy examples/ ./examples/

# Create data directories
RUN mkdir -p /app/data /app/data/logs && \
    chown -R storemy:storemy /app/data


USER storemy

ENV DB_NAME=storemy_db
ENV DATA_DIR=/app/data

# Volume for data persistence
VOLUME ["/app/data"]

# Default command - can be overridden
ENTRYPOINT ["./storemy"]

CMD ["--db", "storemy_db", "--data", "/app/data"]
