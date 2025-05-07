package substreams_sink_kafka

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	pbkafka "github.com/mavvverick/substreams-sink-kafka/pb/sf/substreams/sink/kafka/v1"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type Sink struct {
	*shutter.Shutter
	*sink.Sinker
	logger     *zap.Logger
	client     *kgo.Client
	topic      string
	cursorPath string
}

type Message struct {
	Data        []byte
	Attributes  map[string]string
	OrderingKey string
}

func NewSink(sinker *sink.Sinker, logger *zap.Logger, cursorPath string, client *kgo.Client, topic string) *Sink {
	s := &Sink{
		Shutter:    shutter.New(),
		Sinker:     sinker,
		logger:     logger,
		client:     client,
		cursorPath: cursorPath,
		topic:      topic,
	}

	return s
}

func (s *Sink) Run(ctx context.Context) {
	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.logger.Info("terminating")
		s.Sinker.Shutdown(err)
	})

	cursor, err := s.loadCursor()
	if err != nil {
		s.Shutdown(fmt.Errorf("loading cursor: %w", err))
	}

	s.logger.Info("starting kafka sink", zap.Stringer("restarting_at", cursor.Block()))
	s.Sinker.Run(ctx, cursor, sink.NewSinkerHandlers(s.handleBlockScopedData, s.handleBlockUndoSignal))
}

func (s *Sink) handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {

	publish := &pbkafka.Publish{}

	err := data.Output.MapOutput.UnmarshalTo(publish)
	if err != nil {
		return fmt.Errorf("unmarshalling output: %w", err)
	}

	blockNum := data.Clock.Number
	messages := generateBlockScopedMessages(publish, cursor, blockNum)

	err = s.publishMessages(ctx, messages)
	if err != nil {
		return fmt.Errorf("publishing messages: %w", err)
	}

	err = s.saveCursor(cursor)
	if err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}

	return nil
}

func generateBlockScopedMessages(publish *pbkafka.Publish, cursor *sink.Cursor, blockNum uint64) []*kgo.Record {
	var messages []*kgo.Record

	var indexCounter int
	for _, message := range publish.Messages {
		var headers []kgo.RecordHeader
		for _, attribute := range message.Attributes {
			headers = append(headers, kgo.RecordHeader{
				Key:   attribute.Key,
				Value: []byte(attribute.Value),
			})
		}

		headers = append(headers, kgo.RecordHeader{
			Key:   "Cursor",
			Value: []byte(cursor.String()),
		})

		orderingKey := fmt.Sprintf("%09d_%05d", blockNum, indexCounter)
		msg := &kgo.Record{
			Topic:   "", // Topic is set in publishRecords
			Value:   message.Data,
			Headers: headers,
			Key:     []byte(orderingKey),
		}

		messages = append(messages, msg)
		indexCounter++
	}

	return messages
}

func (s *Sink) handleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	lastValidBlockNum := data.LastValidBlock.Number

	messages := generateUndoBlockMessages(lastValidBlockNum, cursor)

	err := s.publishMessages(ctx, messages)
	if err != nil {
		return fmt.Errorf("publishing messages: %w", err)
	}

	err = s.saveCursor(cursor)
	if err != nil {
		return fmt.Errorf("saving cursor: %w", err)
	}

	return nil
}

func (s *Sink) loadCursor() (*sink.Cursor, error) {
	fpath := filepath.Join(s.cursorPath, "cursor.json")

	_, err := os.Stat(fpath)
	if os.IsNotExist(err) {
		return nil, nil
	}

	cursorData, err := os.ReadFile(fpath)

	if err != nil {
		return nil, fmt.Errorf("reading cursor file: %w", err)
	}

	cursorString := string(cursorData)
	cursor, err := sink.NewCursor(cursorString)
	if err != nil {
		return nil, fmt.Errorf("parsing cursor: %w", err)
	}

	return cursor, nil
}

func (s *Sink) saveCursor(c *sink.Cursor) error {
	cursorString := c.String()

	err := os.MkdirAll(s.cursorPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("making state store path: %w", err)
	}

	fpath := filepath.Join(s.cursorPath, "cursor.json")

	err = os.WriteFile(fpath, []byte(cursorString), os.ModePerm)
	if err != nil {
		return fmt.Errorf("writing cursor file: %w", err)
	}

	return nil
}

func (s *Sink) publishMessages(ctx context.Context, records []*kgo.Record) error {
	for _, record := range records {
		record.Topic = s.topic
	}

	results := s.client.ProduceSync(ctx, records...)
	for _, result := range results {
		if result.Err != nil {
			return fmt.Errorf("publishing record: %w", result.Err)
		}
	}

	return nil
}

func generateUndoBlockMessages(lastValidBlockNum uint64, cursor *sink.Cursor) []*kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: "LastValidBlock", Value: []byte(strconv.FormatUint(lastValidBlockNum, 10))},
		{Key: "Step", Value: []byte("Undo")},
		{Key: "Cursor", Value: []byte(cursor.String())},
	}

	record := &kgo.Record{
		Topic:   "", // Topic is set in publishRecords
		Value:   nil,
		Headers: headers,
	}

	return []*kgo.Record{record}
}
