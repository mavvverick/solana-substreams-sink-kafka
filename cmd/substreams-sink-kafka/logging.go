package main

import (
	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.ApplicationLogger("sink-kafka", "github.com/mavvverick/substreams-sink-kafka/cmd/substreams-sink-kafka")

func init() {
	cli.SetLogger(zlog, tracer)
}
