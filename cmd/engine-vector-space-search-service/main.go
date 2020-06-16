package main

import (
	"flag"

	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/pipeline"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

var (
	_CfgPath = flag.String("conf", "config/pipeline.json", "pipeline config")
)

func main() {
	flag.Parse()

	var pipelieCfg conf.PipelineConfig
	utils.LoadConfigOrPanic(*_CfgPath, &pipelieCfg)

	pipeline.NewContainer(&pipelieCfg)
}
