package controller

import (
	"context"
	"net/http"
	"time"
)

func Health(service interface{ Ready(context.Context) bool }) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/livez":
			w.WriteHeader(http.StatusOK)
		case "/readyz":
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if service.Ready(ctx) {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}
