using System;
using Confluent.Kafka;

public class Driver {
    string kafkaServer = "localhost:9092";

    public static void Main(string[] args) {
        var driver = new Driver();
        driver.run();      
    }

    public void run() {
        produce();
        consume();
    }

    private void produce() {
        var config = new ProducerConfig { BootstrapServers = kafkaServer };
        var producer = new ProducerBuilder<Null, string>(config).Build();
        var topic = "events";
        
        DateTime now = DateTime.Now;
        Event exampleEvent = new Event("0", "example", now.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
        string message = exampleEvent.ToString();
        try {
            var result = producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).Result;
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
        } catch (ProduceException<Null, string> e) {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }

    // consume once
    private void consume() {
        var config = new ConsumerConfig { 
            BootstrapServers = kafkaServer,
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build()) {
            consumer.Subscribe("events");
            var cr = consumer.Consume();
            Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
        }
    }
}

public class Event {
    public string event_id { get; set; }
    public string event_type { get; set; }
    public string event_timestamp { get; set; }

    public Event(string event_id, string event_type, string event_timestamp) {
        this.event_id = event_id;
        this.event_type = event_type;
        this.event_timestamp = event_timestamp;
    }

    public override string ToString() {
        return $"{{\"event_id\":\"{event_id}\",\"event_type\":\"{event_type}\",\"event_timestamp\":\"{event_timestamp}\"}}";
    }
}