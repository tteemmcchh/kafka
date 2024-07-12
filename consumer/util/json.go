package util

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

func WrapJson(values []string) []byte {
	res, err := json.Marshal(values)
	if err != nil {
		log.Error("Error marshaling JSON:", err)
		return nil
	}
	return res
}
