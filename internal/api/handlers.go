package api

import (
	"TestTaskMessagio/internal/kafka"
	"TestTaskMessagio/internal/storage"
	"encoding/json"
	"net/http"
)

type Handler struct {
	db       *storage.PostgresDB
	producer *kafka.Producer
}

func NewHandler(db *storage.PostgresDB, producer *kafka.Producer) *Handler {
	return &Handler{db: db, producer: producer}
}

func (h *Handler) CreateMessage(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Content string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	message, err := h.db.SaveMessage(input.Content)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.producer.SendMessage(message); err != nil {
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	total, processed, err := h.db.GetStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{
		"total":     total,
		"processed": processed,
	})
}
