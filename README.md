# esreindexer

Makes reindexing of existing data in elasticsearch easy.

### Why?

Because you may do poor choices that you regret later.
Reindexing combined with aliases can help you change mapping,
routing or whatever you want to change that requires reindexing.

### Speed

Speed of reindexing is mostly limited by elasticsearch. We use
scrolling to fetch data and parallel bulk indexing for insertion
data back into elasticsearch.

### Usage

Use docker image:

```
docker run --rm -it bobrik/esreindexer
```

It will print command line options to use.

### Authors

* [Ian Babrou](https://github.com/bobrik)
