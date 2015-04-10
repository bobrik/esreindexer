package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/bobrik/esreindexer"
	"github.com/olivere/elastic"
	"golang.org/x/net/context"
)

type esQuery map[string]interface{}

func (q esQuery) Source() interface{} {
	return q
}

func main() {
	src := flag.String("src", "", "source in format http://host:port/index")
	dst := flag.String("dst", "", "destination in format http://host:port/index")
	query := flag.String("query", "{\"match_all\":{}}", "query in json format")
	pool := flag.Int("pool", 5, "how many indexers to start")
	pack := flag.Int("pack", 1000, "scrolling size")
	flag.Parse()

	q := esQuery(map[string]interface{}{})

	err := json.Unmarshal([]byte(*query), &q)
	if err != nil {
		log.Fatalf("query %q is invalid: %s", *query, err)
	}

	srcES, srcIndex, err := urlToES(*src)
	if err != nil {
		log.Fatal("error creating source: ", err)
	}

	dstES, dstIndex, err := urlToES(*dst)
	if err != nil {
		log.Fatal("error creating destination: ", err)
	}

	r, err := esreindexer.NewReindexer(
		esreindexer.SetSource(srcES, srcIndex),
		esreindexer.SetDestination(dstES, dstIndex),
		esreindexer.SetLogger(log.New(os.Stderr, "", log.LstdFlags)),
	)
	if err != nil {
		log.Fatal("error creating reindexer: ", err)
	}

	ctx := context.Background()
	err = r.Run(ctx, *pool, q, "10m", *pack)
	if err != nil {
		log.Fatal("error reindexing: ", err)
	}
}

func clientFromURL(u *url.URL) (*elastic.Client, error) {
	return elastic.NewClient(
		elastic.SetURL(u.Scheme+"://"+u.Host),
		elastic.SetSniff(false),
		elastic.SetMaxRetries(2),
		elastic.SetErrorLog(log.New(os.Stderr, "", log.LstdFlags)),
	)
}

func urlToES(s string) (es *elastic.Client, index string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		err = fmt.Errorf("error parsing %q: %s", s, err)
		return
	}

	if u.Path == "/" || strings.LastIndex(u.Path, "/") != 0 {
		err = fmt.Errorf("index path is invalid: %q", u)
		return
	}

	es, err = clientFromURL(u)
	index = u.Path[1:]

	return
}
