FROM golang:alpine AS build
WORKDIR /app
COPY . .
RUN go mod tidy && \
    echo "start building..." && \
    GOOS=linux GOARCH=amd64 go build -o /cloud-sql-sync .

FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine
RUN apk add --no-cache tzdata jq
ENV TZ=Asia/Taipei
COPY --from=build /cloud-sql-sync /cloud-sql-sync
WORKDIR /
RUN curl -o /bin/cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.0.0/cloud-sql-proxy.linux.amd64 && \
    chmod +x /bin/cloud-sql-* && \
    mkdir -p /cloudsql
ENTRYPOINT ["/cloud-sql-sync"]