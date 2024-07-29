package kafka

import (
	"TestTaskMessagio/internal/models"
	"encoding/json"
	"os"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(kafkaBrokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) SendMessage(message *models.Message) error {
	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: os.Getenv("KAFKA_TOPIC"),
		Value: sarama.StringEncoder(jsonMessage),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
