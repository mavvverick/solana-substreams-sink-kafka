package main

import (
	"fmt"

	skafka "github.com/mavvverick/substreams-sink-kafka"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/manifest"
	"github.com/twmb/franz-go/pkg/kgo"
)

var sinkCmd = Command(sinkRunE,
	"sink <manifest-path> <module-name> <topic-name> [<block-range>]",
	"Substreams Kafka sinking",
	RangeArgs(3, 4),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)

		flags.String("cursor_path", "./state", "Sink cursor's path")
		flags.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers (comma-separated)")
		flags.StringP("endpoint", "e", "", "Substreams gRPC endpoint (e.g. 'mainnet.eth.streamingfast.io:443')")
	}),
	Description(`
		Publishes block data on a Kafka topic from a Substreams output.

		The required arguments are:
		- <manifest-path>: URL or local path to a '.yaml' file (e.g. './examples/simple/substreams.yaml').
		- <module-name>: The module name returning publish instructions in the substreams.
		- <topic-name>: The Kafka topic name to publish the messages to.

		The optional arguments are:
		- <start>:<stop>: The range of block to sync, if not provided, will sync from the module's initial block and then forever.
		                  Can be of the form(s):
						  * <start>: (sync from <start> and forever)
						  * :<stop>: (sync from the module's initial block to <stop>)
						  * <start>:<stop>: (sync from <start> to <stop>)

		If <start>:<stop> is not provided, assumes the whole chain.
	`),
	ExamplePrefixed("substreams-sink-kafka sink", `
		# Publish block data messages produced by map_clocks for the whole chain
		-e mainnet.eth.streamingfast.io:443 ./examples/simple/substreams.yaml map_clocks "topic" --bootstrap-servers "localhost:9092"
		# Publish block data messages produced by map_clocks for a specific range of blocks
		-e mainnet.eth.streamingfast.io:443 ./examples/simple/substreams.yaml map_clocks "topic" 0:1000 --bootstrap-servers "localhost:9092"
	`),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()
	ctx := cmd.Context()

	manifestPath, module, topicName, blockRange := extractInjectArgs(cmd, args)
	endpoint := sflags.MustGetString(cmd, "endpoint")
	cursorPath := sflags.MustGetString(cmd, "cursor_path")
	bootstrapServers := sflags.MustGetString(cmd, "bootstrap-servers")

	// Initialize Kafka client using franz-go
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}

	// FIXME: This is now duplicated across sinkers. It should have
	// definitely be added in sink.NewFromViper directly so that it's shared across all sinkers.
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	sinker, err := sink.NewFromViper(
		cmd,
		"sf.substreams.sink.kafka.v1.Publish",
		endpoint, manifestPath, module, blockRange,
		zlog, tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	s := skafka.NewSink(sinker, zlog, cursorPath, client, topicName)

	s.OnTerminating(func(err error) {
		if err != nil {
			app.Shutdown(err)
			return
		}
	})

	app.OnTerminating(func(err error) {
		s.Shutdown(err)
	})

	s.Run(ctx)

	return nil
}

func extractInjectArgs(_ *cobra.Command, args []string) (manifestPath, moduleName, topicName, blockRange string) {
	manifestPath = args[0]
	moduleName = args[1]

	if len(args) >= 3 {
		topicName = args[2]
	}

	if len(args) == 4 {
		blockRange = args[3]
	}
	return
}
