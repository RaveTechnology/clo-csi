package channel

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	tokenFile  string
	http       *http.Client
	cloudProof chan struct{}
}

func NewClient(baseURL string, caBundle []byte, tokenFile string) (*Client, error) {
	u, e := url.Parse(baseURL)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || tokenFile == "" {
		return nil, protocol.Fail(protocol.Invalid, "HTTPS controller URL and projected token file required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBundle) {
		return nil, protocol.Fail(protocol.Invalid, "valid controller CA bundle required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("controller redirect refused") }}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), tokenFile: tokenFile, http: client, cloudProof: make(chan struct{}, 1)}, nil
}
func (c *Client) request(ctx context.Context, path string, in, out any) error {
	// A complete fresh cloud proof needs several individually paced API reads.
	// Queue these transactions before starting the HTTP deadline. Keep binding,
	// job polling and receipts independent so proof waits cannot deadlock recovery.
	if path == "/register" || path == "/stage" || path == "/report" {
		select {
		case c.cloudProof <- struct{}{}:
			defer func() { <-c.cloudProof }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	raw, e := json.Marshal(in)
	if e != nil {
		return protocol.Fail(protocol.Invalid, "node request encoding failed")
	}
	token, e := os.ReadFile(c.tokenFile)
	if e != nil {
		return protocol.Fail(protocol.Unavailable, "projected node token unavailable")
	}
	bearer := strings.TrimSpace(string(token))
	if bearer == "" {
		return protocol.Fail(protocol.Unavailable, "projected node token empty")
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if e != nil {
		return protocol.Fail(protocol.Invalid, "node request construction failed")
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	response, e := c.http.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return protocol.Fail(protocol.Unavailable, "verified HTTPS controller request failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return protocol.Fail(protocol.NotFound, "no queued node job")
	}
	if response.StatusCode != http.StatusOK {
		var wire wireError
		_ = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&wire)
		code := wire.Code
		switch code {
		case protocol.Invalid, protocol.NotFound, protocol.Conflict, protocol.Blocked, protocol.RecoveryRequired, protocol.Unavailable:
		default:
			code = protocol.Unavailable
		}
		return protocol.Fail(code, "controller rejected node request")
	}
	if out == nil {
		return nil
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(out) != nil {
		return protocol.Fail(protocol.Unavailable, "invalid controller response")
	}
	return nil
}
func (c *Client) Register(ctx context.Context, h protocol.NodeHello) (protocol.NodeRef, error) {
	var out protocol.NodeRef
	e := c.request(ctx, "/register", h, &out)
	return out, e
}
func (c *Client) Binding(ctx context.Context) (protocol.NodeRef, error) {
	var out protocol.NodeRef
	e := c.request(ctx, "/binding", struct{}{}, &out)
	return out, e
}
func (c *Client) AuthorizeStage(ctx context.Context, q protocol.StageRequest) (protocol.StagePermit, error) {
	var out protocol.StagePermit
	e := c.request(ctx, "/stage", q, &out)
	return out, e
}
func (c *Client) Report(ctx context.Context, r protocol.Receipt) error {
	return c.request(ctx, "/report", r, &struct{}{})
}
func (c *Client) Poll(ctx context.Context) (protocol.Job, error) {
	var out protocol.Job
	e := c.request(ctx, "/poll", struct{}{}, &out)
	return out, e
}
func (c *Client) Complete(ctx context.Context, r protocol.Receipt) error {
	return c.request(ctx, "/receipt", r, &struct{}{})
}
func (c *Client) Run(ctx context.Context, execute func(context.Context, protocol.Job) (protocol.Receipt, error)) error {
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		job, e := c.Poll(ctx)
		if e == nil {
			r, runErr := execute(ctx, job)
			if runErr != nil {
				r = protocol.Receipt{JobID: job.ID, Nonce: job.Nonce, Type: job.Type, Node: job.Node, Volume: job.Volume, Error: "node job failed"}
			}
			e = c.Complete(ctx, r)
		}
		if e != nil {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

var _ protocol.Authorizer = (*Client)(nil)
