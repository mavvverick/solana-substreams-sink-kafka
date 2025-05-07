package main

import (
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
)

var version = "dev"

func main() {
	cli.Run("substreams-sink-kafka", "Substreams Kafka sink",
		sinkCmd,

		cli.ConfigureViper("KAFKA_SINK"),
		cli.ConfigureVersion(version),
		cli.OnCommandErrorLogAndExit(zlog),

		cli.PersistentFlags(func(flags *pflag.FlagSet) {
			flags.Duration("delay-before-start", 0, "[OPERATOR] Amount of time to wait before starting any internal processes, can be used to perform to maintenance on the pod before actually letting it starts")
			flags.String("metrics-listen-addr", "localhost:9102", "[OPERATOR] If non-empty, the process will listen on this address for Prometheus metrics request(s)")
			flags.String("pprof-listen-addr", "localhost:6060", "[OPERATOR] If non-empty, the process will listen on this address for pprof analysis (see https://golang.org/pkg/net/http/pprof/)")
		}),
	)
}
