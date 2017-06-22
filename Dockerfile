FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY ./worker .

CMD ["./worker"]
