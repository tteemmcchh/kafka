package repo

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
)

type RepoConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBname   string
}

type Repo struct {
	conn *pgx.Conn
}

func NewRepo(config RepoConfig) *Repo {
	repo := &Repo{}
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", config.User, config.Password, config.Host, config.Port, config.DBname)
	var err error
	repo.conn, err = pgx.Connect(context.Background(), url)
	if err != nil {
		log.Error("ERROR connection database")
		return nil
	}
	return repo
}

func (repo *Repo) CloseConnection(ctx context.Context) {
	err := repo.conn.Close(ctx)
	if err != nil {
		// Handle error
		log.Error("ERROR close connection database")
	}
}

func (repo *Repo) AddValue(ctx context.Context, value string) error {
	query := "INSERT INTO input (value) VALUES ($1)"
	_, err := repo.conn.Exec(ctx, query, value)
	return err
}

func (repo *Repo) ReadValues(ctx context.Context) ([]string, error) {
	var vals []string

	query := "SELECT value FROM input"
	rows, err := repo.conn.Query(ctx, query)
	if err != nil {
		return []string{}, err
	}

	for rows.Next() {
		var value string
		err := rows.Scan(&value)
		if err != nil {
			return []string{}, err
		}
		vals = append(vals, value)
	}

	return vals, nil
}
