package main

import (
	"awesomeProject/producer/client"
	"context"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

func main() {
	brokers := []string{"localhost:29092"}
	kafkaClient := client.NewKafkaClient(brokers, "test")

	http.HandleFunc("/send", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "POST" {
			body, err := ioutil.ReadAll(request.Body)
			if err != nil {
				log.Errorf("Error reading body:", err)
				writer.WriteHeader(http.StatusInternalServerError)
				_, err := writer.Write([]byte("Internal error"))
				if err != nil {
					log.Error("Error writing response:", err)
				}
				return
			}

			log.Info("Info: Received body:", string(body))
			go func() {
				err := kafkaClient.SendMessage(context.Background(), body)
				if err != nil {
					log.Error("Error sending message to Kafka:", err)
				}
			}()

			_, err = writer.Write([]byte("Record sent"))
			if err != nil {
				log.Error("Error writing response:", err)
			}
		}
	})

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Error("Error starting server:", err)
	}
}
