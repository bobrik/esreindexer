package esreindexer

import (
	"fmt"
	"log"
	"sync"

	"github.com/olivere/elastic"
	"golang.org/x/net/context"
)

// ReindexerOptionFunc is a function that configures reindexer
type ReindexerOptionFunc func(*Reindexer) error

// Reindexer represents process of moving data from one index to another
type Reindexer struct {
	srcES *elastic.Client
	dstES *elastic.Client

	srcIndex string
	dstIndex string

	logger *log.Logger
}

// NewReindexer creates new reindexer with specified options
func NewReindexer(opts ...ReindexerOptionFunc) (*Reindexer, error) {
	r := &Reindexer{}

	for _, opt := range opts {
		err := opt(r)
		if err != nil {
			return nil, err
		}
	}

	return r, nil
}

// SetSource sets source elasticsearch client and index
func SetSource(es *elastic.Client, index string) ReindexerOptionFunc {
	return func(r *Reindexer) error {
		r.srcES = es
		r.srcIndex = index
		return nil
	}
}

// SetDestination sets destination elasticsearch client and index
func SetDestination(es *elastic.Client, index string) ReindexerOptionFunc {
	return func(r *Reindexer) error {
		r.dstES = es
		r.dstIndex = index
		return nil
	}
}

// SetLogger sets optional logger for reindexer
func SetLogger(logger *log.Logger) ReindexerOptionFunc {
	return func(r *Reindexer) error {
		r.logger = logger
		return nil
	}
}

// Run reindexes data with pool of workers, defined query, timeout and chunk size
func (r *Reindexer) Run(ctx context.Context, pool int, query elastic.Query, timeout string, size int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(pool)

	reqCh := make(chan []elastic.BulkableRequest, pool)
	errCh := make(chan error, 1)

	r.listen(ctx, wg, reqCh, errCh)

	r.suck(ctx, reqCh, errCh, query, timeout, size)
	close(reqCh)

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// listen starts listening for data to index
func (r *Reindexer) listen(ctx context.Context, wg *sync.WaitGroup, ch <-chan []elastic.BulkableRequest, errCh chan<- error) {
	for i := 0; i < cap(ch); i++ {
		r.log(fmt.Sprintf("Started listener #%d", i))

		go func(i int) {
			defer wg.Done()

			for {
				select {
				case reqs, ok := <-ch:
					if !ok {
						return
					}

					r.log(fmt.Sprintf("Got %d docs to index in listener #%d", len(reqs), i))

					b := r.dstES.Bulk().Index(r.dstIndex)
					for _, req := range reqs {
						b.Add(req)
					}

					_, err := b.Do()
					if err != nil {
						writeErrorMaybe(errCh, err)
						return
					}
				case <-ctx.Done():
					writeErrorMaybe(errCh, ctx.Err())
					r.log(fmt.Sprintf("context for listener #%d is done: %s", i, ctx.Err()))
					return
				}
			}
		}(i)
	}
}

// suck starts pulling data out of elasticsearch and does reindexing
func (r *Reindexer) suck(ctx context.Context, ch chan<- []elastic.BulkableRequest, errCh chan<- error, query elastic.Query, timeout string, size int) {
	r.log("Starting reindexing")

	cursor, err := r.srcES.Scan(r.srcIndex).
		KeepAlive(timeout).
		Size(size).
		Query(query).
		Do()
	if err != nil {
		writeErrorMaybe(errCh, err)
		return
	}

	if cursor.TotalHits() == 0 {
		return
	}

	fetched := 0

	for {
		if ctx.Err() != nil {
			writeErrorMaybe(errCh, ctx.Err())
			return
		}

		res, err := cursor.Next()
		if err != nil {
			writeErrorMaybe(errCh, err)
			return
		}

		if len(res.Hits.Hits) == 0 {
			return
		}

		fetched += len(res.Hits.Hits)

		r.log(fmt.Sprintf("Got %d (%d/%d) docs from scroll", len(res.Hits.Hits), fetched, cursor.TotalHits()))

		ch <- r.hitsToDocs(res.Hits.Hits)
	}
}

// hitsToDocs converts *elastic.SearchHit structs to elastic.BulkableRequest for indexing
func (r *Reindexer) hitsToDocs(hits []*elastic.SearchHit) []elastic.BulkableRequest {
	requests := make([]elastic.BulkableRequest, len(hits))

	for i, hit := range hits {
		requests[i] = elastic.NewBulkIndexRequest().
			Type(hit.Type).
			Id(hit.Id).
			Doc(*hit.Source)
	}

	return requests
}

// log writes message to log package if logging is enabled
func (r *Reindexer) log(message string) {
	if r.logger == nil {
		return
	}

	r.logger.Println(message)
}

// writeErrorMaybe tries to write error to the channel
// and returns immediately if the channel is full
func writeErrorMaybe(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}
