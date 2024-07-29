package main

import (
	"log"
	"net/http"

	"TestTaskMessagio/internal/api"
	"TestTaskMessagio/internal/kafka"
	"TestTaskMessagio/internal/storage"

	"github.com/gorilla/mux"
)

func main() {
	// Инициализация подключения к PostgreSQL
	db, err := storage.NewPostgresDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Инициализация Kafka producer
	kafkaProducer, err := kafka.NewProducer()
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()
	// Инициализация Kafka consumer
	kafkaConsumer, err := kafka.NewConsumer(db)
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
	log.Println("Server is running on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
