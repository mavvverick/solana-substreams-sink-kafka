package substreams_sink_kafka

// import (
// 	"context"
// 	"fmt"
// 	"sort"
// 	"sync"
// 	"testing"
// 	"time"

// 	"github.com/streamingfast/bstream"
// 	sink "github.com/streamingfast/substreams-sink"

// 	"github.com/twmb/franz-go/pkg/kgo"
// 	// "cloud.google.com/go/kgo"
// 	// "cloud.google.com/go/kgo/pstest"
// 	"github.com/streamingfast/logging"
// 	"github.com/streamingfast/shutter"
// 	"github.com/stretchr/testify/require"
// 	"google.golang.org/api/option"
// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/codes"
// 	"google.golang.org/grpc/status"
// )

// var logger, _ = logging.ApplicationLogger("test", "test")

// func TestHandleCursor(t *testing.T) {
// 	bstreamCursor := bstream.Cursor{
// 		Step:      1,
// 		Block:     bstream.NewBlockRefFromID("3"),
// 		LIB:       bstream.NewBlockRefFromID("2"),
// 		HeadBlock: bstream.NewBlockRefFromID("4"),
// 	}

// 	cursor := &sink.Cursor{
// 		Cursor: &bstreamCursor,
// 	}

// 	testSink := &Sink{
// 		Shutter:    shutter.New(),
// 		Sinker:     nil,
// 		logger:     logger,
// 		client:     nil,
// 		topic:      "topic",
// 		cursorPath: "/tmp/sink-sate",
// 	}

// 	err := testSink.saveCursor(cursor)
// 	require.NoError(t, err)

// 	loadCursor, err := testSink.loadCursor()
// 	require.NoError(t, err)

// 	require.Equal(t, loadCursor, cursor)

// }

// type resultMessage struct {
// 	data        string
// 	attributes  map[string]string
// 	orderingKey string
// }

// func TestPublishMessages(t *testing.T) {
// 	ctx := context.Background()

// 	cases := []struct {
// 		name            string
// 		messages        []*kgo.Record
// 		expectedResults []resultMessage
// 	}{
// 		{
// 			name: "sunny path",
// 			messages: []*kgo.Record{
// 				{
// 					Data: []byte("data.1"),
// 				},
// 			},
// 			expectedResults: []resultMessage{
// 				{
// 					data: "data.1",
// 				},
// 			},
// 		},
// 		{
// 			name: "multiple messages",
// 			messages: []*kgo.Record{
// 				{
// 					Data:        []byte("data.1"),
// 					OrderingKey: "1_1",
// 				},
// 				{
// 					Data:        []byte("data.2"),
// 					OrderingKey: "1_2",
// 				},
// 				{
// 					Data:        []byte("data.99"),
// 					OrderingKey: "1_3",
// 				},
// 			},
// 			expectedResults: []resultMessage{
// 				{
// 					data:        "data.1",
// 					orderingKey: "1_1",
// 				},
// 				{
// 					data:        "data.2",
// 					orderingKey: "1_2",
// 				},
// 				{
// 					data:        "data.99",
// 					orderingKey: "1_3",
// 				},
// 			},
// 		},
// 		{
// 			name: "multiple complex messages",
// 			messages: []*kgo.Message{
// 				{
// 					Data:        []byte("data.1"),
// 					OrderingKey: "1_1",
// 					Attributes:  map[string]string{"cursor": "2"},
// 				},
// 				{
// 					Data:        []byte("data.2"),
// 					OrderingKey: "1_2",
// 					Attributes:  map[string]string{"cursor": "3"},
// 				},
// 				{
// 					Data:        []byte("data.99"),
// 					OrderingKey: "1_3",
// 					Attributes:  map[string]string{"cursor": "4"},
// 				},
// 			},
// 			expectedResults: []resultMessage{
// 				{
// 					data:        "data.1",
// 					orderingKey: "1_1",
// 					attributes:  map[string]string{"cursor": "2"},
// 				},
// 				{
// 					data:        "data.2",
// 					orderingKey: "1_2",
// 					attributes:  map[string]string{"cursor": "3"},
// 				},
// 				{
// 					data:        "data.99",
// 					orderingKey: "1_3",
// 					attributes:  map[string]string{"cursor": "4"},
// 				},
// 			},
// 		},
// 		{
// 			name: "undo message",
// 			messages: []*kgo.Message{
// 				{
// 					Data:       nil,
// 					Attributes: map[string]string{"LastValidBlock": "4", "Step": "Undo", "Cursor": "1"},
// 				},
// 			},
// 			expectedResults: []resultMessage{
// 				{
// 					data:       "",
// 					attributes: map[string]string{"LastValidBlock": "4", "Step": "Undo", "Cursor": "1"},
// 				},
// 			},
// 		},
// 	}

// 	for _, c := range cases {
// 		t.Run(c.name, func(t *testing.T) {
// 			// Start a fake server running locally.
// 			srv := pstest.NewServer()
// 			defer srv.Close()

// 			// Connect to the server without using TLS.
// 			conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
// 			require.NoError(t, err)

// 			defer conn.Close()
// 			client, err := kgo.NewClient(ctx, "project", option.WithGRPCConn(conn))
// 			defer client.Close()

// 			topic, err := client.CreateTopic(ctx, "topic")
// 			if err != nil {
// 				require.NoError(t, err)
// 			}

// 			topic.EnableMessageOrdering = true

// 			testSink := &Sink{
// 				Shutter:    shutter.New(),
// 				Sinker:     nil,
// 				logger:     logger,
// 				client:     client,
// 				topic:      topic,
// 				cursorPath: "",
// 			}

// 			subscription, err := client.CreateSubscription(ctx, "sub", kgo.SubscriptionConfig{
// 				Topic:                 testSink.topic,
// 				AckDeadline:           10 * time.Second,
// 				EnableMessageOrdering: true,
// 			})
// 			if err != nil {
// 				require.NoError(t, err)
// 			}

// 			expectedResultCount := len(c.expectedResults)
// 			var results []resultMessage
// 			var lock sync.Mutex
// 			done := make(chan interface{})

// 			go func() {
// 				err = subscription.Receive(ctx, func(ctx context.Context, m *kgo.Message) {
// 					lock.Lock()
// 					results = append(results, resultMessage{
// 						data:        string(m.Data),
// 						attributes:  m.Attributes,
// 						orderingKey: m.OrderingKey,
// 					})

// 					if len(results) == expectedResultCount {
// 						fmt.Println("Closing done channel")
// 						close(done)
// 						fmt.Println("channel closed")
// 					}
// 					m.Ack()
// 					lock.Unlock()
// 				})
// 				if err != nil {
// 					if s, ok := status.FromError(err); ok {
// 						if s.Code() == codes.Canceled {
// 							return
// 						}
// 					}

// 					fmt.Printf("Error: %T\n", err)
// 					require.NoError(t, err)
// 				}
// 			}()

// 			err = testSink.publishMessages(ctx, c.messages)
// 			require.NoError(t, err)

// 			expire := time.After(1 * time.Second)
// 			select {
// 			case <-done:
// 			case <-expire:
// 				t.Fatal(fmt.Sprintf("Timeout: no message received"))
// 			}

// 			//pstest doesn't support message ordering
// 			sort.Slice(results, func(i, j int) bool {
// 				return results[i].orderingKey < results[j].orderingKey
// 			})

// 			require.Equal(t, c.expectedResults, results)
// 		})
// 	}
// }

// func TestGenerateBlockScopedMessages(t *testing.T) {

// 	cursor := &sink.Cursor{
// 		Cursor: &bstream.Cursor{
// 			Step:      1,
// 			Block:     bstream.NewBlockRefFromID("3"),
// 			LIB:       bstream.NewBlockRefFromID("2"),
// 			HeadBlock: bstream.NewBlockRefFromID("4"),
// 		},
// 	}

// 	blockNumber := uint64(4)
// 	publish := &pbkgo.Publish{
// 		Messages: []*pbkgo.Message{
// 			{
// 				Data:       []byte("data.1"),
// 				Attributes: []*pbkgo.Attribute{{Key: "key1", Value: "value1"}},
// 			},
// 			{
// 				Data:       []byte("data.2"),
// 				Attributes: []*pbkgo.Attribute{{Key: "key2", Value: "value2"}},
// 			},
// 		},
// 	}

// 	expectedResults := []*kgo.Message{
// 		{
// 			Data: []byte("data.1"),
// 			Attributes: map[string]string{
// 				"Cursor": "e_jb3d3LppwOzpSs-jtHy6WyLpcyBlBsXwvvLhtBj4k=",
// 				"key1":   "value1",
// 			},
// 			OrderingKey: "000000004_00000",
// 		},
// 		{
// 			Data: []byte("data.2"),
// 			Attributes: map[string]string{
// 				"Cursor": "e_jb3d3LppwOzpSs-jtHy6WyLpcyBlBsXwvvLhtBj4k=",
// 				"key2":   "value2",
// 			},
// 			OrderingKey: "000000004_00001",
// 		},
// 	}

// 	results := generateBlockScopedMessages(publish, cursor, blockNumber)

// 	require.Equal(t, expectedResults, results)
// }

// func TestGenerateUndoBlockMessages(t *testing.T) {

// 	cursor := &sink.Cursor{
// 		Cursor: &bstream.Cursor{
// 			Step:      1,
// 			Block:     bstream.NewBlockRefFromID("3"),
// 			LIB:       bstream.NewBlockRefFromID("2"),
// 			HeadBlock: bstream.NewBlockRefFromID("4"),
// 		},
// 	}

// 	LastValidBlockNumber := uint64(4)

// 	expectedResults := []*kgo.Message{
// 		{
// 			Data: nil,
// 			Attributes: map[string]string{
// 				"Cursor":         "e_jb3d3LppwOzpSs-jtHy6WyLpcyBlBsXwvvLhtBj4k=",
// 				"LastValidBlock": "4",
// 				"Step":           "Undo",
// 			},
// 		},
// 	}

// 	results := generateUndoBlockMessages(LastValidBlockNumber, cursor)

// 	require.Equal(t, expectedResults, results)
// }
