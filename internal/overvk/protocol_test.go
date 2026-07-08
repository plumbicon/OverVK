package overvk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUploadDocumentPreservesPostAcrossRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("ParseMultipartForm() error = %v", err)
		}
		if _, _, err := r.FormFile("file"); err != nil {
			t.Fatalf("FormFile(file) error = %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"file":"uploaded-file-token"}`))
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	result, err := uploadDocument(context.Background(), redirect.Client(), redirect.URL, []byte("payload"))
	if err != nil {
		t.Fatalf("uploadDocument() error = %v", err)
	}
	if result["file"] != "uploaded-file-token" {
		t.Fatalf("file = %v, want uploaded-file-token", result["file"])
	}
}
