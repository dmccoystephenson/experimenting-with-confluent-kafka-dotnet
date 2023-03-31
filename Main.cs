using System;
using Confluent.Kafka;

class MainClass
{
    public static void Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
        var producer = new ProducerBuilder<Null, string>(config).Build();
        var dr = producer.ProduceAsync("test", new Message<Null, string> { Value = "test" }).Result;
        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");       
    }
}