package handler

import (
	"net/http"
	"vk_tech/consumer/repo"
	"vk_tech/consumer/util"
)

func ListHandler(repository *repo.Repo) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
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
		_, err = writer.Write(util.WrapJson(values))
		if err != nil {
			// handle error
		}
	}
}

//http.HandleFunc("/list", func (writer http.ResponseWriter, request *http.Request) {
//	values, err := repository.ReadValues(request.Context())
//	if err != nil {
//		writer.WriteHeader(500)
//		_, err := writer.Write([]byte("Internal server error"))
//		if err != nil {
//			// handle error
//		}
//		return
//	}
//
//	writer.WriteHeader(200)
//	_, err = writer.Write(wrapJson(values))
//	if err != nil {
//		// handle error
//	}
//})
