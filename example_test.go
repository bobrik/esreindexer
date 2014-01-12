package esreindexer_test

import "log"
import "github.com/belogik/goes"
import reindexer "github.com/bobrik/esreindexer"

func ExampleReindexer() {
	// connection to elasticsearch
	c := goes.NewConnection("127.0.0.1", "9200")

	// moving data from source_index to destination_index
	// with 5 simultaneous bulk indexing requests
	r := reindexer.NewReindexer(c, "source_index", "destination index", 5)

	// logging is disabled by default
	r.Log = true

	// query object to filter data
	q := map[string]interface{}{
		"query": map[string]interface{}{
			"filtered": map[string]interface{}{
				"filter": map[string]interface{}{
					"term": map[string]interface{}{
						"something": 8,
					},
				},
			},
		},
	}

	// listen should be called to index data
	r.Listen()

	// suck pulls data from elasticsearch
	err := r.Suck(q, "1m", 1000)
	if err != nil {
		log.Fatal(err)
	}

	// close resources
	r.Close()

	log.Println("Finished")
}
