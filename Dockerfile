FROM alpine:3.3

COPY . /go/src/github.com/bobrik/esreindexer

RUN apk --update add go && \
    GOPATH=/go GO15VENDOREXPERIMENT=1 go get github.com/bobrik/esreindexer/cmd/esreindexer && \
    apk del go && \
    cp /go/bin/esreindexer /bin/esreindexer && \
    rm -rf /go

ENTRYPOINT ["/bin/esreindexer"]
