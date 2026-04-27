FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /csi-driver ./cmd/driver

FROM debian:bookworm-slim
COPY --from=builder /csi-driver /csi-driver
ENTRYPOINT ["/csi-driver"] 
# a
