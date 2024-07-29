package storage

import (
	"TestTaskMessagio/internal/models"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	db *sql.DB
}

func GetPostgresConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_DB"),
	)
}

func NewPostgresDB(connStr string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// Создаем таблицу, если она не существует
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `)
	if err != nil {
		return nil, err
	}

	return &PostgresDB{db: db}, nil
}

func (p *PostgresDB) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
func (p *PostgresDB) SaveMessage(content string) (*models.Message, error) {
	var message models.Message
	err := p.db.QueryRow(
		"INSERT INTO messages (content) VALUES ($1) RETURNING id, content, processed, created_at",
		content,
	).Scan(&message.ID, &message.Content, &message.Processed, &message.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &message, nil
}

func (p *PostgresDB) MarkAsProcessed(id int64) error {
	_, err := p.db.Exec("UPDATE messages SET processed = TRUE WHERE id = $1", id)
	return err
}

func (p *PostgresDB) GetStats() (int64, int64, error) {
	var total, processed int64
	err := p.db.QueryRow("SELECT COUNT(*), COUNT(*) FILTER (WHERE processed = TRUE) FROM messages").Scan(&total, &processed)
	if err != nil {
		return 0, 0, err
	}
	return total, processed, nil
}
