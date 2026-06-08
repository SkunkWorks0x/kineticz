FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kineticz ./cmd/kineticz

# Bake the Phoenix MCP server. The Go process spawns it as a node subprocess at
# runtime for the diagnose self-introspection leg; no npx at runtime.
FROM node:22-bookworm-slim AS mcp
WORKDIR /app
RUN npm install --omit=dev @arizeai/phoenix-mcp@4.0.13

# distroless nodejs22 supplies node at /nodejs/bin/node (see internal/phoenix
# NodeDialer). Entrypoint stays the Go binary; node runs only as a child.
FROM gcr.io/distroless/nodejs22-debian12
COPY --from=builder /out/kineticz /kineticz
COPY --from=mcp /app/node_modules /app/node_modules
ENV PORT=8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/kineticz"]
