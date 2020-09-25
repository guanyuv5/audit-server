package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"k8s.io/apiserver/pkg/apis/audit"
)

//WebhookServer xxx
type WebhookServer struct {
	server *http.Server
}

var (
	port        int
	broker      string
	kafkaClient sarama.SyncProducer
)

func init() {
	var err error
	flag.IntVar(&port, "port", 8888, "Webhook server port.")
	flag.StringVar(&broker, "broker", "127.0.0.1:9200", "File containing the x509 Certificate for HTTPS.")
	flag.Parse()
	fmt.Println("kafka brokers: ", broker)
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	kafkaClient, err = sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connect kafka")
}

func (whsvr *WebhookServer) serve(w http.ResponseWriter, r *http.Request) {
	var el = audit.EventList{}
	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	json.Unmarshal(body, &el)
	for i := range el.Items {
		msg := &sarama.ProducerMessage{}
		msg.Topic = "mykafka"
		js, _ := json.Marshal(el.Items[i])
		msg.Value = sarama.StringEncoder(string(js))
		pid, offset, err := kafkaClient.SendMessage(msg)
		if err != nil {
			fmt.Println("send message failed,", err)
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		fmt.Printf("pid:%v offset:%v\n", pid, offset)
	}
	fmt.Fprintf(w, "%d", http.StatusOK)
}

func main() {

	whsvr := &WebhookServer{
		server: &http.Server{
			Addr: fmt.Sprintf(":%v", port),
		},
	}

	// define http server and server handler
	mux := http.NewServeMux()
	mux.HandleFunc("/audit", whsvr.serve)
	whsvr.server.Handler = mux

	// start webhook server in new routine
	go func() {
		if err := whsvr.server.ListenAndServe(); err != nil {
			fmt.Printf("Failed to listen and serve webhook server: %v", err)
		}
	}()

	fmt.Println("Server started")

	// listening OS shutdown singal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	fmt.Println("Got OS shutdown signal, shutting down webhook server gracefully...")
	whsvr.server.Shutdown(context.Background())
	kafkaClient.Close()

}
