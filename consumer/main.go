package consumer

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"vk_tech/consumer/client"
	"vk_tech/consumer/handler"
	"vk_tech/consumer/repo"
)

type TDocument struct {
	Url            string
	PubDate        uint64
	FetchTime      uint64
	Text           string
	FirstFetchTime uint64
}

func main() {
	repository := repo.NewRepo(repo.RepoConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "example",
		DBname:   "postgres",
	})

	kafkaClient := client.NewKafkaClient([]string{"localhost:29092"}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		for {
			select {

			case <-ctx.Done():
				log.Println("Done listening")
				return

			default:
				msg, err := kafkaClient.Read(ctx)
				if err != nil {
					log.Error("Couldn't read from kafka ", err)
					continue
				}
				// func processor (msg) return {pretty message}

				err = repository.AddValue(ctx, string(msg))
				if err != nil {
					fmt.Println("Couldn't write kafka msg to DB " + err.Error())
				}

			}
		}
	}(ctx)
	http.HandleFunc("/list", handler.ListHandler(repository))

	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Error("Error in Listening server at :9090", err)
		// handle error
	}
}
