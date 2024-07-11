package main

import (
	"awesomeProject/consumer/client"
	"awesomeProject/consumer/repo"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

func wrapJson(values []string) []byte {
	res, err := json.Marshal(values)
	if err != nil {
		log.Error("Error marshaling JSON:", err)
		return nil
	}
	return res
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
			case <-time.After(500 * time.Millisecond):
				msg, err := kafkaClient.Read(ctx)
				if err != nil {
					fmt.Println("Couldn't read from kafka " + err.Error())
					continue
				}

				err = repository.AddValue(ctx, string(msg))
				if err != nil {
					fmt.Println("Couldn't write kafka msg to DB " + err.Error())
				}

			case <-ctx.Done():
				fmt.Println("Done listening")
				return
			}
		}
	}(ctx)

	http.HandleFunc("/list", func(writer http.ResponseWriter, request *http.Request) {
		values, err := repository.ReadValues(request.Context())
		if err != nil {
			writer.WriteHeader(500)
			_, err := writer.Write([]byte("Internal server error"))
			if err != nil {
				// handle error
			}
			return
		}

		writer.WriteHeader(200)
		_, err = writer.Write(wrapJson(values))
		if err != nil {
			// handle error
		}
	})

	err := http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Error(err)
		// handle error
	}

}
