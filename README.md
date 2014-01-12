esreindexer
===

Makes reindexing of existing data in elasticsearch easy.

### Why?

Because you may do poor choices that you regret later.
Reindexing combined with aliases can help you change mapping,
routing or whatever you want to change that requires reindexing.

This is "let's try to do something in go" kind of project.

### Speed

Speed of reindexing is mostly limited by elasticsearch. We use
scrolling to fetch data and parallel bulk indexing for insertion
data back into elasticsearch.

### Usage

See `example_test.go` example, usage is pretty straightforward.

### Authors

* [Ian Babrou](https://github.com/bobrik)
