# Extism Kafka Consumer

> **Note**: This is not currently meant to be a finished product itself but rather an experiment in
>  how you could allow customers to extend your Kafka cluster. 

One interesting use case for Extism is to allow users to respond to events in your infrastructure. Something like
webhooks but, the compute is inside your infrastructure instead remotely in your customer's.

Most companies have embraced Kafka as a tool to build these kind of event based architectures and they are running
their own clusters or using hosted services such as [Confluent.io](https://www.confluent.io/).

This repo explores the idea of extending parts of the Kafka ecosystem with Extism plug-ins. Specifically this
allows you to run an Extism plug-in as a [Kafka Consumer](https://kafka.apache.org/25/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html).
You could also do the same thing with the [Producer](https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) or any other parts of the Kafka ecosystem.

This is also to experiment and improve upon the [Host Functions](https://extism.org/docs/concepts/host-functions/) interface. We need the plugins to be able to invoke some of the Kafka Consumer API (e.g. `Consumer#Commit()`).

## Using

You need docker, rust, and go.

```bash
git clone git@github.com/extism/extism-kafka-consumer.git
cd extism-kafka-consumer

# build the rust plugin:
cd plugin/ && cargo build --target wasm32-unknown-unknown --release && cd ..

# build the consumer
go build

# spin up kafka using docker compose
docker compose up -d

# create a topic called weather:
docker compose exec broker \
    kafka-topics --create \
        --topic weather \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1

# run the consumer
./kafka-consumer
```

In another tab, open a producer and feed some events into it:

```bash
docker compose exec broker bash
kafka-console-producer --topic weather --broker-list broker:9092
>72 
>73
>74
>
```
