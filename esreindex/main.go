package main

import "flag"
import "net/url"
import "log"
import "github.com/bobrik/esreindexer"
import "github.com/belogik/goes"
import "strings"
import "encoding/json"

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

	srcUrl, err := url.Parse(*src)
	if err != nil {
		log.Fatal("error parsing", src, err)
	}

	dstUrl, err := url.Parse(*dst)
	if err != nil {
		log.Fatal("error parsing", dst, err)
	}

	if srcUrl.Path == "/" || dstUrl.Path == "/" {
		log.Fatal("indices are invalid")
	}

	srcEs := urlToEs(srcUrl)
	dstEs := urlToEs(dstUrl)

	q := map[string]interface{}{}

	err = json.Unmarshal([]byte(*query), &q)
	if err != nil {
		log.Fatal("query is invalid", err)
	}

	r := esreindexer.NewReindexer(srcEs, dstEs, srcUrl.Path[1:], dstUrl.Path[1:], *pool)
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
