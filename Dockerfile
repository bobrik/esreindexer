FROM alpine:3.1

ADD . /go/src/github.com/bobrik/esreindexer

ENV GOPATH=/go:/go/src/github.com/bobrik/esreindexer/cmd/esreindexer/Godeps/_workspace
RUN apk --update add go && \
    go get github.com/bobrik/esreindexer/cmd/esreindexer && \
    apk del go && \
    cp /go/bin/esreindexer /bin/esreindexer && \
    rm -rf /go

ENTRYPOINT ["/bin/esreindexer"]
