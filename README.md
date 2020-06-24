# engine-vector-indexing-service

[![Build Status](http://img.shields.io/travis/badges/badgerbadgerbadger.svg?style=flat-square)](https://travis-ci.org/badges/badgerbadgerbadger) [![Coverage Status](http://img.shields.io/coveralls/badges/badgerbadgerbadger.svg?style=flat-square)](https://coveralls.io/r/badges/badgerbadgerbadger) [![License](http://img.shields.io/:license-mit-blue.svg?style=flat-square)](http://badges.mit-license.org) 

## Architecture

![arch](doc/arch.jpeg)

## Key Algorithms

### Inverted Indexing

![inverted-indexing](doc/inverted-indexing.jpeg)

### TF-IDF and Similarity Computation

* **doc vector**

![doc-vector](doc/doc_vector.jpeg)

* **doc vector weight**

![doc-vector-weight](doc/doc_vector_weight.jpeg)

* **query vector**

![query-vector](doc/query_vector.jpeg)

* **query vector weight**

![query-vector-weight](doc/query_vector_weight.jpeg)

* **similarity**

![similarity](doc/similarity.jpeg)

## Get Started

### Prerequisites

* OS: Linux
* Golang: v1.12+

### Installation

#### Clone

* Clone this repo to your local machine using https://github.com/amazingchow/engine-vector-space-search-service.git.

#### Setup

```shell
# build the binary
make build

# start the service
./engine-vector-space-search-service --conf=config/pipeline.json --debug=false
```

#### Example

use crawler system to produce raw html docs, after engine-vector-space-search-service consumes all html docs, then  

```shell
# get system info
curl http://127.0.0.1:18180/v1/system_info

# do query
curl -XPOST -d '{"query": "Hello World", "topk": 3}' http://127.0.0.1:18180/v1/query
```

## Documentation

### Api Design

* refer to [swagger](pb/engine-vector-space-search-service.swagger.json).

### References

* [A Vector Space Model for Automatic Indexing](https://dl.acm.org/doi/pdf/10.1145/361219.361220?download=true)
* [Design and Implementation of a Search Engine using Vector Space and PageRank](https://guangchun.files.wordpress.com/2012/05/searchenginereport.pdf)
* [Inverted Index Implementation](https://is.muni.cz/th/hsr4u/thesis.pdf)

## Contributing

### Step 1

* 🍴 Fork this repo!

### Step 2

* 🔨 HACK AWAY!

### Step 3

* 🔃 Create a new PR using https://github.com/amazingchow/engine-vector-space-search-service/compare!

## FAQ

* refer to [FAQ](FAQ.md).

## Support

* Reach out to me at <jianzhou42@163.com>.

## License

* This project is licensed under the MIT License - see the **[MIT license](http://opensource.org/licenses/mit-license.php)** for details.
