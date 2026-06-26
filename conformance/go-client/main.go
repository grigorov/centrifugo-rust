// A live conformance check using the real centrifuge-go v0.8.4 SDK (the client
// for the protocol-v0.3.4 / seq-gen era this server targets). It connects to the
// Rust server given as argv[1], subscribes to a channel, publishes a message,
// and waits to receive it back as a publication. Prints "OK" and exits 0 on
// success; prints "FAIL: ..." and exits 1 otherwise.
//
// This is the strongest compatibility proof: an unmodified real client SDK
// speaks to the Rust binary end to end.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/centrifugal/centrifuge-go"
)

type handler struct {
	connected  chan struct{}
	subscribed chan struct{}
	received   chan []byte
	errs       chan string
}

func (h *handler) OnConnect(_ *centrifuge.Client, e centrifuge.ConnectEvent) {
	if e.ClientID == "" {
		h.errs <- "connect: empty client id"
		return
	}
	close(h.connected)
}
func (h *handler) OnError(_ *centrifuge.Client, e centrifuge.ErrorEvent) {
	h.errs <- "client error: " + e.Message
}
func (h *handler) OnDisconnect(_ *centrifuge.Client, e centrifuge.DisconnectEvent) {
	// Only an unexpected disconnect (non-clean) is a failure signal here.
	if e.Reason != "" && !e.Reconnect {
		h.errs <- "disconnected: " + e.Reason
	}
}
func (h *handler) OnSubscribeSuccess(_ *centrifuge.Subscription, _ centrifuge.SubscribeSuccessEvent) {
	close(h.subscribed)
}
func (h *handler) OnPublish(_ *centrifuge.Subscription, e centrifuge.PublishEvent) {
	h.received <- e.Data
}

func fail(msg string) {
	fmt.Printf("FAIL: %s\n", msg)
	os.Exit(1)
}

func waitFor(name string, ch <-chan struct{}, errs <-chan string) {
	select {
	case <-ch:
	case e := <-errs:
		fail(e)
	case <-time.After(5 * time.Second):
		fail("timeout waiting for " + name)
	}
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: goclient <ws-url>")
	}
	url := os.Args[1]

	h := &handler{
		connected:  make(chan struct{}),
		subscribed: make(chan struct{}),
		received:   make(chan []byte, 1),
		errs:       make(chan string, 8),
	}

	client := centrifuge.New(url, centrifuge.DefaultConfig())
	defer client.Close()
	client.OnConnect(h)
	client.OnError(h)
	client.OnDisconnect(h)

	// Optional connection JWT (argv[2]) exercises the token auth path.
	if len(os.Args) >= 3 && os.Args[2] != "" {
		client.SetToken(os.Args[2])
	}

	if err := client.Connect(); err != nil {
		fail("connect: " + err.Error())
	}
	waitFor("connect", h.connected, h.errs)

	sub, err := client.NewSubscription("bench")
	if err != nil {
		fail("new subscription: " + err.Error())
	}
	sub.OnSubscribeSuccess(h)
	sub.OnPublish(h)
	if err := sub.Subscribe(); err != nil {
		fail("subscribe: " + err.Error())
	}
	waitFor("subscribe", h.subscribed, h.errs)

	if _, err := sub.Publish([]byte(`{"msg":"hello from centrifuge-go"}`)); err != nil {
		fail("publish: " + err.Error())
	}

	select {
	case data := <-h.received:
		if want := `"hello from centrifuge-go"`; !contains(string(data), want) {
			fail(fmt.Sprintf("publication data mismatch: %s", string(data)))
		}
		fmt.Println("OK")
	case e := <-h.errs:
		fail(e)
	case <-time.After(5 * time.Second):
		fail("timeout waiting for publication")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
