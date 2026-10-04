FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /telemetry ./cmd/telemetry

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /telemetry /telemetry
COPY schemas /schemas
ENV SCHEMA_DIR=/schemas
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/telemetry"]
