FROM golang:1.22 as builder

WORKDIR /app
 
COPY . .

RUN go mod download

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

RUN go build -ldflags="-s -w" -o main main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates mysql mysql-client

WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/main .

RUN chmod +x main

# Expose port 3000 to the outside world
EXPOSE 3000

# Command to run the executable
CMD ["./main"]