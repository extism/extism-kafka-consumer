package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/cgo"
	"unsafe"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/extism/extism"
)

/*
#include <extism.h>
EXTISM_GO_FUNCTION(consumer_commit);
*/
import "C"

type ConsumerPlugin struct {
	ctx      *extism.Context
	plugin   *extism.Plugin
	filePath string
}

type SubscribeTopics struct {
	Topics []string `json:"topics"`
}

//export consumer_commit
func consumer_commit(plugin unsafe.Pointer, inputs *C.ExtismVal, nInputs C.ExtismSize, outputs *C.ExtismVal, nOutputs C.ExtismSize, userData uintptr) {
	fmt.Println("Hello from Go!")
	s := cgo.Handle(userData)
	fmt.Println(s)
	fmt.Println(s.Value())

	// consumer := s.Value().(*kafka.Consumer)
	// consumer.Commit()
	inputSlice := unsafe.Slice(inputs, nInputs)
	outputSlice := unsafe.Slice(outputs, nOutputs)

	// Get memory pointed to by first element of input slice
	p := extism.GetCurrentPlugin(plugin)
	mem := p.Memory(extism.ValGetUInt(unsafe.Pointer(&inputSlice[0])))
	fmt.Println(string(mem))

	outputSlice[0] = inputSlice[0]
}

func NewConsumerPlugin(filePath string, consumer *kafka.Consumer) (*ConsumerPlugin, error) {
	manifest := extism.Manifest{Wasm: []extism.Wasm{extism.WasmFile{Path: filePath}}}
	ctx := extism.NewContext()
	commitFunc := extism.NewFunction("consumer_commit", []extism.ValType{extism.I64}, []extism.ValType{extism.I64}, C.consumer_commit, consumer)
	defer commitFunc.Free()
	plugin, err := ctx.PluginFromManifest(manifest, []extism.Function{commitFunc}, false)
	if err != nil {
		return nil, err
	}
	return &ConsumerPlugin{
		ctx:      &ctx,
		plugin:   &plugin,
		filePath: filePath,
	}, nil
}

type Poll struct {
	Time int `json:"time_ms"`
}

func (c *ConsumerPlugin) Poll() (int, error) {
	result, err := c.plugin.Call("poll", nil)
	if err != nil {
		return -1, err
	}
	var poll Poll
	json.Unmarshal(result, &poll)
	return poll.Time, nil
}

func (c *ConsumerPlugin) ConfigMap() (map[string]string, error) {
	result, err := c.plugin.Call("config_map", nil)
	if err != nil {
		return nil, err
	}
	var cm map[string]string
	json.Unmarshal(result, &cm)
	return cm, nil
}

func (c *ConsumerPlugin) SubscribeTopics() (*SubscribeTopics, error) {
	result, err := c.plugin.Call("subscribe_topics", nil)
	if err != nil {
		return nil, err
	}
	topics := SubscribeTopics{}
	json.Unmarshal(result, &topics)
	return &topics, nil
}

func (c *ConsumerPlugin) OnMessage(message *kafka.Message) error {
	extismMsg, err := NewPluginMessage(message)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(extismMsg)
	if err != nil {
		return err
	}

	fmt.Printf("Payload: %s\n", string(payload))

	resp, err := c.plugin.Call("on_message", payload)
	if err != nil {
		return err
	}

	fmt.Println(string(resp))

	return nil
}

func (c *ConsumerPlugin) OnError() error {
	if !c.plugin.FunctionExists("on_error") {
		return nil
	}
	// TODO implement
	return nil
}

func (c *ConsumerPlugin) OnDefault() error {
	if !c.plugin.FunctionExists("on_default") {
		return nil
	}
	// TODO implement
	return nil
}

type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type TopicPartition struct {
	Topic     *string `json:"topic"`
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	Metadata  *string `json:"metadata"`
	Error     *string `json:"error"`
}

type Message struct {
	TopicPartition TopicPartition `json:"topic_partition"`
	Value          string         `json:"value"`
	Key            string         `json:"key"`
	Timestamp      string         `json:"timestamp"`
	Headers        []Header       `json:"headers"`
}

func NewPluginMessage(message *kafka.Message) (*Message, error) {
	var topicPartitionError *string
	if message.TopicPartition.Error != nil {
		err := message.TopicPartition.Error.Error()
		topicPartitionError = &err
	}
	var headers []Header
	for _, h := range message.Headers {
		headers = append(headers, Header{Key: string(h.Key), Value: string(h.Value)})
	}

	msg := &Message{
		TopicPartition: TopicPartition{
			Topic:     message.TopicPartition.Topic,
			Partition: message.TopicPartition.Partition,
			Offset:    int64(message.TopicPartition.Offset),
			Metadata:  message.TopicPartition.Metadata,
			Error:     topicPartitionError,
		},
		Value:     string(message.Value),
		Key:       string(message.Key),
		Timestamp: message.Timestamp.String(),
		Headers:   headers,
	}
	return msg, nil
}

func main() {
	var consumer *kafka.Consumer

	//args := os.Args[1:]
	plugin, err := NewConsumerPlugin("./plugin/target/wasm32-unknown-unknown/release/plugin.wasm", consumer)
	if err != nil {
		panic(err)
	}

	cm, err := plugin.ConfigMap()
	if err != nil {
		panic(err)
	}

	kcm := kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	for k, v := range cm {
		kcm.SetKey(k, v)
	}
	fmt.Println("Config map: ", kcm)

	consumer, err = kafka.NewConsumer(&kcm)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	fmt.Println("attached consumer to plugin")

	topics, err := plugin.SubscribeTopics()
	if err != nil {
		panic(err)
	}

	fmt.Println("Subscribing to: ", topics.Topics)
	err = consumer.SubscribeTopics(topics.Topics, nil)
	if err != nil {
		panic(err)
	}

	run := true
	for run {
		pt, err := plugin.Poll()
		if err != nil {
			panic(err)
		}

		ev := consumer.Poll(pt)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
			plugin.OnMessage(e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			//fmt.Printf("Ignored %v\n", e)
		}
	}
}
