package main

import (
	"context"
	"log"
	"sync"

	"github.com/DTSL/golang-libraries/errorhandle"
	"github.com/DTSL/golang-libraries/errors"
	"github.com/DTSL/golang-libraries/kafkautils"
	"github.com/DTSL/golang-libraries/timeutils"
	"github.com/DTSL/golang-libraries/tracingmain"
	"github.com/DTSL/log-pipeline/chmodel"
	kafkalogs "github.com/DTSL/log-pipeline/kafka-logs"
	"github.com/mailru/easyjson"
	"github.com/segmentio/kafka-go"
	otel "go.opentelemetry.io/otel/trace"
)

func runKafka(ctx context.Context, dic *diContainer) error {
	readerCfg, err := getKafkaReaderConfig(dic)
	if err != nil {
		return errors.Wrap(err, "get reader config")
	}
	pr, err := dic.kafkaProcessor()
	if err != nil {
		return errors.Wrap(err, "get processor")
	}
	kafkautils.RunBatchConsumers(ctx, readerCfg, pr.process, dic.flags.kafkaConsumers, dic.flags.kafkaBatchsize, dic.flags.kafkaBatchDelay, errorhandle.HandleDefault)
	return nil
}

func getKafkaReaderConfig(dic *diContainer) (kafka.ReaderConfig, error) {
	cfg, err := kafkalogs.GetConfig(dic.flags.environment)
	if err != nil {
		return kafka.ReaderConfig{}, errors.Wrap(err, "config")
	}
	readerCfg := cfg.NewReaderConfig()
	readerCfg.GroupID = "log-ingestion-sanitizer"
	readerCfg.Topic = kafkalogs.FluentdEnrichTopic
	readerCfg.WatchPartitionChanges = true
	readerCfg.ErrorLogger = kafka.LoggerFunc(func(format string, args ...interface{}) {
		log.Printf("Kafka reader error: "+format, args...)
	})
	return readerCfg, nil
}

type kafkaProcessor struct {
	decoder   func(ctx context.Context, kmsgs []kafka.Message) (*kafkalogs.ExtendedEnrichMessages, error)
	processor func(ctx context.Context, ingMsgs *kafkalogs.ExtendedEnrichMessages) error

	tracer otel.Tracer
}

func newKafkaProcessor(dic *diContainer) (*kafkaProcessor, error) {
	dec, err := dic.kafkaDecoder()
	if err != nil {
		return nil, errors.Wrap(err, "decoder")
	}
	pr, err := dic.processor()
	if err != nil {
		return nil, errors.Wrap(err, "processor")
	}
	return &kafkaProcessor{
		decoder:   dec.decodeMessages,
		processor: pr.process,
		tracer:    tracingmain.GetOtelTracer(),
	}, nil
}

func newKafkaProcessorDIProvider(dic *diContainer) func() (*kafkaProcessor, error) {
	var p *kafkaProcessor
	var mu sync.Mutex
	return func() (_ *kafkaProcessor, err error) {
		mu.Lock()
		defer mu.Unlock()
		if p == nil {
			p, err = newKafkaProcessor(dic)
		}
		return p, err
	}
}

func (p *kafkaProcessor) process(ctx context.Context, kmsgs []kafka.Message) error {
	_, span := p.tracer.Start(ctx, "kafka.process")
	defer span.End()

	logData, err := p.decoder(ctx, kmsgs)
	if err != nil {
		return errors.Wrap(err, "decode")
	}
	if logData == nil {
		return nil
	}

	err = p.processor(ctx, logData)
	if err != nil {
		return errors.Wrap(err, "process")
	}
	return nil
}

type kafkaDecoder struct {
	errFunc      func(context.Context, error)
	deadProducer kafkautils.Producer
	tracer       otel.Tracer
}

func newKafkaDecoder(dic *diContainer) (*kafkaDecoder, error) {
	producer, err := dic.kafka.ProducerSingle()
	if err != nil {
		return nil, errors.Wrap(err, "producer")
	}
	return &kafkaDecoder{
		errFunc:      errorhandle.HandleDefault,
		deadProducer: producer.Produce,
		tracer:       tracingmain.GetOtelTracer(),
	}, nil
}

func newKafkaDecoderDIProvider(dic *diContainer) func() (*kafkaDecoder, error) {
	var v *kafkaDecoder
	var mu sync.Mutex
	return func() (_ *kafkaDecoder, err error) {
		mu.Lock()
		defer mu.Unlock()
		if v == nil {
			v, err = newKafkaDecoder(dic)
		}
		return v, err
	}
}

func (d *kafkaDecoder) decodeMessages(ctx context.Context, kmsgs []kafka.Message) (*kafkalogs.ExtendedEnrichMessages, error) {
	kmsgsB := []byte("[")

	lenKmsgs := len(kmsgs)

	// , separated []byte to unmarshal it at once
	for i, kmsg := range kmsgs {
		if i != (lenKmsgs - 1) {
			kmsg.Value = append(kmsg.Value, []byte(",")...)
		}
		kmsgsB = append(kmsgsB, kmsg.Value...)
	}

	kmsgsB = append(kmsgsB, []byte("]")...)

	var logData kafkalogs.EnrichMessages

	err := easyjson.Unmarshal(kmsgsB, &logData)
	if err != nil {
		err = d.handleInvalidMessages(ctx, err, kmsgsB, kmsgs[0].Headers)
		if err != nil {
			return nil, errors.Wrap(err, "handle invalid messages")
		}
		return nil, errors.Wrap(err, "unmarshaling kafka msgs")
	}

	createTime := timeutils.Now()
	if lenKmsgs > 0 {
		createTime = kmsgs[0].Time
	}

	extendedLogData := kafkalogs.ExtendedEnrichMessages{
		Content:    &logData,
		CreateTime: createTime.In(chmodel.ClickhouseTimeZone),
	}

	return &extendedLogData, nil
}

func (d *kafkaDecoder) handleInvalidMessages(ctx context.Context, err error, kmsgValues []byte, headers []kafka.Header) error {
	err = errors.Wrap(err, "invalid message")
	err = errors.Wrap(err, "Kafka decoder")
	d.errFunc(ctx, err)

	kDeadMsgs := kafka.Message{
		Topic:   kafkalogs.FluentdEnrichDeadTopic,
		Value:   kmsgValues,
		Headers: headers,
	}

	err = d.deadProducer(ctx, kDeadMsgs)
	if err != nil {
		return errors.Wrap(err, "dead producer")
	}
	return nil
}
