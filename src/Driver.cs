using System;
using Confluent.Kafka;

class Driver {
    string kafkaServer = "localhost:9092";

    public static void Main(string[] args) {
        var driver = new Driver();
        driver.run();      
    }

    public void run() {
        produce();
    }

    private void produce() {
        var config = new ProducerConfig { BootstrapServers = kafkaServer };
        var producer = new ProducerBuilder<Null, string>(config).Build();
        var topic = "test";
        var message = "test message";
        try {
            var result = producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).Result;
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
        } catch (ProduceException<Null, string> e) {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }
}