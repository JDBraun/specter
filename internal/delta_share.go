package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

type DeltaShareConfig struct {
	ShareCredentialsVersion int    `json:"shareCredentialsVersion"`
	BearerToken             string `json:"bearerToken"`
	Endpoint                string `json:"endpoint"`
	ExpirationTime          string `json:"expirationTime"`
}

type RequestBody struct {
	PredicateHints     []string `json:"predicateHints,omitempty"`
	LimitHint          int      `json:"limitHint,omitempty"`
	Version            int      `json:"version,omitempty"`
	JSONPredicateHints string   `json:"jsonPredicateHints,omitempty"`
}

type FileResponse struct {
	File struct {
		URL string `json:"url"`
	} `json:"file"`
}

func FetchDeltaPreSignedURL(share, schema, table string, predicateHints []string, limitHint int) (string, error) {
	config, err := ReadConfig("config/config.share")
	if err != nil {
		log.Fatalf("Error reading Delta config: %v", err)
	}

	url := fmt.Sprintf("%s/shares/%s/schemas/%s/tables/%s/query", config.Endpoint, share, schema, table)

	requestBody := &RequestBody{
		PredicateHints: predicateHints,
		LimitHint:      limitHint,
	}

	requestBodyData, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBodyData))
	if err != nil {
		return "", fmt.Errorf("failed to create new request: %v", err)
	}

	req.Header.Set("Authorization", "Bearer "+config.BearerToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)

	var fileResponse FileResponse
	for {
		if err := decoder.Decode(&fileResponse); err == io.EOF {
			break
		} else if err != nil {
			return "", fmt.Errorf("error decoding response: %v", err)
		}

		if fileResponse.File.URL != "" {
			return fileResponse.File.URL, nil
		}
	}

	return "", fmt.Errorf("no pre-signed URL found in response")
}

func ReadConfig(filePath string) (*DeltaShareConfig, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	var config DeltaShareConfig
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %v", err)
	}

	return &config, nil
}
