FROM golang:1.22.0-alpine

WORKDIR /app

RUN apk add --no-cache mysql mysql-client
 
COPY . .
# RUN go mod download

EXPOSE 8080

# CMD ["go", "run", "main.go"]