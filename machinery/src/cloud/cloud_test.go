package cloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kerberos-io/agent/machinery/src/models"
)

func TestVerifyPersistenceKerberosHubUsesAuthorizationHead(t *testing.T) {
	var requestMethod string
	var requestBody []byte
	var requestHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestMethod = r.Method
		requestHeaders = r.Header.Clone()
		requestBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := models.Config{
		Cloud:         "kerberoshub",
		Key:           "camera-1",
		Name:          "front-door",
		HubURI:        server.URL,
		HubKey:        "project-public",
		HubPrivateKey: "project-private",
		S3:            &models.S3{Region: "eu"},
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/persistence/verify", bytes.NewReader(payload))
	ctx.Request.Header.Set("Content-Type", "application/json")

	VerifyPersistence(ctx, t.TempDir(), server.URL, "")

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if requestMethod != http.MethodHead {
		t.Fatalf("Hub request method = %q, want HEAD", requestMethod)
	}
	if len(requestBody) != 0 {
		t.Fatalf("Hub request body has %d bytes, want none", len(requestBody))
	}
	for header, want := range map[string]string{
		"X-Kerberos-Storage-Device":  "camera-1",
		"X-Kerberos-Hub-PublicKey":   "project-public",
		"X-Kerberos-Hub-PrivateKey":  "project-private",
		"X-Kerberos-Hub-Region":      "eu",
		"X-Kerberos-Storage-Capture": "IPCamera",
	} {
		if got := requestHeaders.Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
}

func TestVerifyPersistenceKerberosHubRejectsCrossHostRedirect(t *testing.T) {
	var redirectedHeaders http.Header
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var originalHeaders http.Header
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originalHeaders = r.Header.Clone()
		http.Redirect(w, r, target.URL+"/storage/upload", http.StatusTemporaryRedirect)
	}))
	defer hub.Close()

	config := models.Config{
		Cloud:         "kerberoshub",
		Key:           "camera-1",
		Name:          "front-door",
		HubURI:        hub.URL,
		HubKey:        "project-public",
		HubPrivateKey: "project-private",
		S3:            &models.S3{Region: "eu"},
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/persistence/verify", bytes.NewReader(payload))
	ctx.Request.Header.Set("Content-Type", "application/json")

	VerifyPersistence(ctx, t.TempDir(), hub.URL, "")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	for _, header := range []string{"X-Kerberos-Hub-PublicKey", "X-Kerberos-Hub-PrivateKey"} {
		if originalHeaders.Get(header) == "" {
			t.Errorf("%s was not sent to the configured Hub", header)
		}
		if redirectedHeaders != nil {
			t.Errorf("redirect target received a request with %s=%q", header, redirectedHeaders.Get(header))
		}
	}
}

func TestVerifyPersistenceKerberosHubRejectsUnsavedURI(t *testing.T) {
	requestCount := 0
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer untrusted.Close()

	config := models.Config{
		Cloud:         "kerberoshub",
		Key:           "camera-1",
		Name:          "front-door",
		HubURI:        untrusted.URL,
		HubKey:        "project-public",
		HubPrivateKey: "project-private",
		S3:            &models.S3{Region: "eu"},
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/persistence/verify", bytes.NewReader(payload))
	ctx.Request.Header.Set("Content-Type", "application/json")

	VerifyPersistence(ctx, t.TempDir(), "https://saved-hub.example.com", "")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if requestCount != 0 {
		t.Fatalf("untrusted Hub received %d requests, want none", requestCount)
	}
}

func TestVerifyHubRejectsUnsavedURI(t *testing.T) {
	requestCount := 0
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer untrusted.Close()

	config := models.Config{
		HubURI:        untrusted.URL,
		HubKey:        "project-public",
		HubPrivateKey: "project-private",
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/hub/verify", bytes.NewReader(payload))
	ctx.Request.Header.Set("Content-Type", "application/json")

	VerifyHub(ctx, "https://saved-hub.example.com")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if requestCount != 0 {
		t.Fatalf("untrusted Hub received %d requests, want none", requestCount)
	}
}

func vaultVerifyContext(t *testing.T, path string, config models.Config) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}

func vaultStorage(uri string) *models.KStorage {
	return &models.KStorage{
		URI:             uri,
		AccessKey:       "vault-access",
		SecretAccessKey: "vault-secret",
		Provider:        "minio",
		Directory:       "camera-1",
	}
}

func TestVerifyPersistenceVaultRejectsUnsavedURI(t *testing.T) {
	requestCount := 0
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer untrusted.Close()

	ctx, recorder := vaultVerifyContext(t, "/api/persistence/verify", models.Config{
		Cloud:    "kerberosvault",
		KStorage: vaultStorage(untrusted.URL),
	})

	VerifyPersistence(ctx, t.TempDir(), "", "https://saved-vault.example.com")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if requestCount != 0 {
		t.Fatalf("untrusted Vault received %d requests, want none", requestCount)
	}
}

func TestVerifySecondaryPersistenceVaultRejectsUnsavedURI(t *testing.T) {
	requestCount := 0
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer untrusted.Close()

	ctx, recorder := vaultVerifyContext(t, "/api/persistence/secondary/verify", models.Config{
		Cloud:             "kerberosvault",
		KStorageSecondary: vaultStorage(untrusted.URL),
	})

	VerifySecondaryPersistence(ctx, t.TempDir(), "")

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if requestCount != 0 {
		t.Fatalf("untrusted Vault received %d requests, want none", requestCount)
	}
}

func TestVerifyPersistenceVaultUsesSavedURI(t *testing.T) {
	var pingHeaders http.Header
	vault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			pingHeaders = r.Header.Clone()
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer vault.Close()

	ctx, _ := vaultVerifyContext(t, "/api/persistence/verify", models.Config{
		Cloud:    "kerberosvault",
		KStorage: vaultStorage(vault.URL + "/"),
	})

	VerifyPersistence(ctx, t.TempDir(), "", vault.URL)

	if pingHeaders == nil {
		t.Fatal("saved Vault did not receive the /ping request")
	}
	if got := pingHeaders.Get("X-Kerberos-Storage-AccessKey"); got != "vault-access" {
		t.Fatalf("X-Kerberos-Storage-AccessKey = %q, want %q", got, "vault-access")
	}
}

func TestVerifySecondaryPersistenceVaultRejectsCrossHostRedirect(t *testing.T) {
	var redirectedHeaders http.Header
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	vault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer vault.Close()

	ctx, recorder := vaultVerifyContext(t, "/api/persistence/secondary/verify", models.Config{
		Cloud:             "kerberosvault",
		KStorageSecondary: vaultStorage(vault.URL),
	})

	VerifySecondaryPersistence(ctx, t.TempDir(), vault.URL)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if redirectedHeaders != nil {
		t.Fatalf("redirect target received a request with X-Kerberos-Storage-SecretAccessKey=%q", redirectedHeaders.Get("X-Kerberos-Storage-SecretAccessKey"))
	}
}

func TestHubPersistenceVerificationErrorExplainsStatus(t *testing.T) {
	for _, test := range []struct {
		status int
		want   string
	}{
		{status: http.StatusBadRequest, want: "credentials or device/project assignment"},
		{status: http.StatusUnauthorized, want: "no active subscription"},
		{status: http.StatusForbidden, want: "device is muted"},
		{status: http.StatusBadGateway, want: "HTTP 502"},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			if got := hubPersistenceVerificationError(test.status); !strings.Contains(got, test.want) {
				t.Fatalf("hubPersistenceVerificationError(%d) = %q, want it to contain %q", test.status, got, test.want)
			}
		})
	}
}

func TestHeartbeatFailureLogOmitsHubResponseBody(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Body:       io.NopCloser(strings.NewReader(`{"error":"invalid heartbeat"}`)),
	}

	responseBody, truncated, err := readHeartbeatResponseBody(response)
	if err != nil {
		t.Fatalf("readHeartbeatResponseBody() error = %v", err)
	}
	fields := heartbeatFailureLogFields(response, responseBody, truncated, 125*time.Millisecond)

	for key, want := range map[string]interface{}{
		"duration_ms":             int64(125),
		"response_body_bytes":     len(responseBody),
		"response_body_truncated": false,
		"status_code":             http.StatusBadRequest,
	} {
		if got := fields[key]; got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	for key, value := range fields {
		if strings.Contains(key, "response_body") && key != "response_body_bytes" && key != "response_body_truncated" {
			t.Fatalf("unexpected response body field %q=%v", key, value)
		}
		if strings.Contains(fmt.Sprint(value), "invalid heartbeat") {
			t.Fatalf("heartbeat log fields exposed response body in %q=%v", key, value)
		}
	}
}

func TestReadHeartbeatResponseBodyTruncatesLargeBody(t *testing.T) {
	response := &http.Response{
		Body: io.NopCloser(strings.NewReader(strings.Repeat("x", heartbeatResponseBodyLogLimit+1))),
	}

	body, truncated, err := readHeartbeatResponseBody(response)
	if err != nil {
		t.Fatalf("readHeartbeatResponseBody() error = %v", err)
	}
	if !truncated {
		t.Fatal("readHeartbeatResponseBody() truncated = false, want true")
	}
	if len(body) != heartbeatResponseBodyLogLimit {
		t.Fatalf("len(body) = %d, want %d", len(body), heartbeatResponseBodyLogLimit)
	}
}
