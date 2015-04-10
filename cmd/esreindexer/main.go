package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/url"
	"strings"

	"github.com/belogik/goes"
	"github.com/bobrik/esreindexer"
)

func main() {
	src := flag.String("src", "", "source in format http://host:port/index")
	dst := flag.String("dst", "", "destination in format http://host:port/index")
	query := flag.String("query", "{}", "query in json format")
	pool := flag.Int("pool", 5, "how many indexers to start")
	pack := flag.Int("pack", 1000, "scrolling size")
	flag.Parse()

	if *src == "" || *dst == "" {
		flag.PrintDefaults()
		return
	}

	srcURL, err := url.Parse(*src)
	if err != nil {
		log.Fatal("error parsing", src, err)
	}

	dstURL, err := url.Parse(*dst)
	if err != nil {
		log.Fatal("error parsing", dst, err)
	}

	if srcURL.Path == "/" || dstURL.Path == "/" {
		log.Fatal("indices are invalid")
	}

	srcEs := urlToEs(srcURL)
	dstEs := urlToEs(dstURL)

	q := map[string]interface{}{}

	err = json.Unmarshal([]byte(*query), &q)
	if err != nil {
		log.Fatal("query is invalid", err)
	}

	r := esreindexer.NewReindexer(srcEs, dstEs, srcURL.Path[1:], dstURL.Path[1:], *pool)
	r.Log = true

	r.Listen()

	// suck pulls data from elasticsearch
	err = r.Suck(q, "10m", *pack)
	if err != nil {
		log.Fatal(err)
	}

	// close resources
	r.Close()
}

func urlToEs(u *url.URL) *goes.Connection {
	p := strings.Split(u.Host, ":")
	return goes.NewConnection(p[0], p[1])
}
