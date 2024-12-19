package ump

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func doPost(url string, body []byte, headers map[string]string) (resultBody []byte, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	req, err = http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return
	}
	for key, header := range headers {
		req.Header.Set(key, header)
	}
	resp, err = client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	resultBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("http status code: %d, response: %v", resp.StatusCode, resultBody)
	}
	return
}
