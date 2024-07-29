package main

import (
	"log"
	"net/http"
	"os"

	"TestTaskMessagio/internal/api"
	"TestTaskMessagio/internal/kafka"
	"TestTaskMessagio/internal/storage"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

func main() {
	// Загрузка .env файла
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Инициализация подключения к PostgreSQL
	postgresConnStr := storage.GetPostgresConnectionString()
	db, err := storage.NewPostgresDB(postgresConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database connection: %v", err)
		}
	}()

	// Инициализация Kafka producer
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	kafkaProducer, err := kafka.NewProducer(kafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Инициализация Kafka consumer
	kafkaConsumer, err := kafka.NewConsumer(db, kafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// Запуск Kafka consumer в отдельной горутине
	go kafkaConsumer.Consume()

	// Инициализация обработчиков API
	handler := api.NewHandler(db, kafkaProducer)

	// Настройка маршрутов
	r := mux.NewRouter()
	r.HandleFunc("/messages", handler.CreateMessage).Methods("POST")
	r.HandleFunc("/stats", handler.GetStats).Methods("GET")

	// Запуск сервера
	serverPort := os.Getenv("SERVER_PORT")
	log.Printf("Server is running on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, r))
}
