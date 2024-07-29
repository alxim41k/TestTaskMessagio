package kafka

import (
	"TestTaskMessagio/internal/models"
	"TestTaskMessagio/internal/storage"
	"encoding/json"
	"log"
	"os"

	"github.com/IBM/sarama"
)

type Consumer struct {
	consumer sarama.Consumer
	db       *storage.PostgresDB
}

func NewConsumer(db *storage.PostgresDB, kafkaBrokers []string) (*Consumer, error) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(kafkaBrokers, config)
	if err != nil {
		return nil, err
	}
	return &Consumer{consumer: consumer, db: db}, nil
}

func (c *Consumer) Consume() {
	partitionConsumer, err := c.consumer.ConsumePartition(os.Getenv("KAFKA_TOPIC"), 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to start consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var message models.Message
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}

			if err := c.db.MarkAsProcessed(message.ID); err != nil {
				log.Printf("Error marking message as processed: %v", err)
			}
		case err := <-partitionConsumer.Errors():
			log.Printf("Error from consumer: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
