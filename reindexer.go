package esreindexer

import "github.com/belogik/goes"
import "sync"
import "log"
import "fmt"

// Reindexer represents process of moving data from one index to another
type Reindexer struct {
	srcEs *goes.Connection
	dstEs *goes.Connection

	ch chan []goes.Document
	wg *sync.WaitGroup

	src string
	dst string

	done chan error

	// Log what's happening into log package
	Log bool
}

// NewReindexer creates new reindexer with specified connections and indices
// pool parameter identifies how many parallel indexing requests are allowed
func NewReindexer(srcEs, dstEs *goes.Connection, src, dst string, pool int) *Reindexer {
	return &Reindexer{
		srcEs: srcEs,
		dstEs: dstEs,
		ch:    make(chan []goes.Document, pool),
		wg:    &sync.WaitGroup{},
		src:   src,
		dst:   dst,
		done:  make(chan error, pool),
	}
}

// Listen starts listening for data to index
func (r *Reindexer) Listen() {
	for i := 0; i < cap(r.ch); i++ {
		r.log(fmt.Sprintf("Started listener #%d", i))
		go func(i int) {
			for docs := range r.ch {
				r.log(fmt.Sprintf("Got %d docs to index in listened #%d", len(docs), i))

				dstDocs := make([]goes.Document, 0, len(docs))
				for _, doc := range docs {
					doc.Index = r.dst
					dstDocs = append(dstDocs, doc)
				}

				_, err := r.dstEs.BulkSend(dstDocs)
				if err != nil {
					r.done <- err
				}

				r.wg.Done()
			}
		}(i)
	}
}

// Suck starts pulling data out of elasticsearch and does reindexing
func (r *Reindexer) Suck(query map[string]interface{}, timeout string, size int) error {
	r.log("starting reindexing")
	scan, err := r.srcEs.Scan(query, []string{r.src}, []string{}, timeout, size)
	if err != nil {
		return err
	}

	if scan.Hits.Total == 0 {
		return err
	}

	r.log(fmt.Sprintf("Found %d docs", scan.Hits.Total))

	for {
		response, err := r.srcEs.Scroll(scan.ScrollId, timeout)
		if err != nil {
			return err
		}

		if len(response.Hits.Hits) == 0 {
			break
		}

		r.log(fmt.Sprintf("Got %d docs from scroll", len(response.Hits.Hits)))

		r.wg.Add(1)

		r.ch <- r.hitsToDocs(response.Hits.Hits)

		select {
		case err := <-r.done:
			if err != nil {
				return err
			}
		default:
			// meh
		}
	}

	r.wg.Wait()

	return nil
}

// Close stops channels after work is done
func (r *Reindexer) Close() {
	close(r.ch)
	close(r.done)
}

// hitsToDocs converts goes.Hit structs to goes.Document structs for reindexing
func (r *Reindexer) hitsToDocs(hits []goes.Hit) []goes.Document {
	result := []goes.Document{}

	for _, hit := range hits {
		doc := goes.Document{
			BulkCommand: "index",
			Index:       hit.Index,
			Type:        hit.Type,
			Id:          hit.Id,
			Fields:      hit.Source,
		}

		result = append(result, doc)
	}

	return result
}

// log writes message to log package if logging is enabled
func (r *Reindexer) log(message string) {
	if !r.Log {
		return
	}

	log.Println(message)
}
