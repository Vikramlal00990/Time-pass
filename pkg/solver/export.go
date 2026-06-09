package solver

import "net/http"

func NewChromeTransport(proxyURL string) (*http.Client, error) {
	transport, err := newChromeTransport(proxyURL)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: transport,
	}, nil
}
