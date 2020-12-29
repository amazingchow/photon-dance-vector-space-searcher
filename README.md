# photon-dance-vector-space-searcher

![arch](doc/arch.jpeg)

## Get Started

### Prerequisites

```text
* OS     : Linux
* Golang : v1.12+
```

### Installation

#### Clone

* Clone this repo to your local machine using https://github.com/amazingchow/photon-dance-vector-space-searcher.git.

#### Setup

```shell
# build the binary
make build

# start the service
./vector-space-searcher --conf=config/pipeline.json --debug=false
```

#### Example

use crawler system to produce raw html docs, after vector-space-searcher consumes all html docs, then  

```shell
# get system info
curl http://127.0.0.1:18180/v1/system_info

# do query
curl -XPOST -d '{"query": "Hello World", "topk": 3}' http://127.0.0.1:18180/v1/query
```

## Documentation

### Api Design

* refer to [swagger](pb/photon-dance-vector-space-searcher.swagger.json).

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

* 🔃 Create a new PR using https://github.com/amazingchow/photon-dance-vector-space-searcher/compare!

## Support

* Reach out to me at <jianzhou42@163.com>.

## License

* This project is licensed under the MIT License - see the **[MIT license](http://opensource.org/licenses/mit-license.php)** for details.
