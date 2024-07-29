package kafka

import (
	"TestTaskMessagio/internal/models"
	"encoding/json"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer() (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, config)
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
		Topic: "messages",
		Value: sarama.StringEncoder(jsonMessage),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
