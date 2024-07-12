package processor

import (
	"context"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
)

type TDocument struct {
	Url            string
	PubDate        uint64
	FetchTime      uint64
	Text           string
	FirstFetchTime uint64
}

type Processor interface {
	Process(d *TDocument) (*TDocument, error)
}

type DocumentProcessor struct {
	conn *pgx.Conn
}

func NewDocumentProcessor(conn *pgx.Conn) *DocumentProcessor {
	return &DocumentProcessor{
		conn: conn,
	}
}

func (p *DocumentProcessor) Process(d *TDocument) (*TDocument, error) {
	ctx := context.Background()
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		log.Error("Cannot to connect DB in Process", err)
		return nil, err
	}

	defer tx.Rollback(ctx)

	var existingDoc TDocument

	err = tx.QueryRow(ctx, "SELECT url, pub_date, fetch_time, text, first_fetch_time FROM test_table WHERE url = $1", d.Url).Scan(
		&existingDoc.Url, &existingDoc.PubDate, &existingDoc.FetchTime, &existingDoc.Text, &existingDoc.FirstFetchTime)

	switch {

	case err == pgx.ErrNoRows:
		// Если документа еще нет, добавляем его
		d.FirstFetchTime = d.FetchTime
		_, err = tx.Exec(ctx, "INSERT INTO test_table (url, pub_date, fetch_time, text, first_fetch_time) VALUES ($1, $2, $3, $4, $5)",
			d.Url, d.PubDate, d.FetchTime, d.Text, d.FirstFetchTime)
		if err != nil {
			log.Error("Cannot to INSERT INTO test_table", err)
			return nil, err
		}

	case err != nil:
		log.Error("Cannot to get Query from test_table", err)
		return nil, err

	default:
		// Обновляем поля
		if d.FetchTime > existingDoc.FetchTime {
			existingDoc.Text = d.Text
			existingDoc.FetchTime = d.FetchTime
		}
		if d.FetchTime < existingDoc.FirstFetchTime {
			existingDoc.FirstFetchTime = d.FetchTime
			existingDoc.PubDate = d.PubDate
		}
		if d.FetchTime < existingDoc.PubDate {
			existingDoc.PubDate = d.PubDate
		}

		_, err = tx.Exec(ctx, "UPDATE documents SET pub_date = $1, fetch_time = $2, text = $3, first_fetch_time = $4 WHERE url = $5",
			existingDoc.PubDate, existingDoc.FetchTime, existingDoc.Text, existingDoc.FirstFetchTime, existingDoc.Url)
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &existingDoc, nil
}
