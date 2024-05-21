using Newtonsoft.Json;
using Azure.Messaging.ServiceBus;
using ServiceBusQueue;
using System.Threading.Tasks;
using System.ComponentModel;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.Configuration;
using System;

var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

string? connectionString = configuration["AppSettings:QueueConnectionString"];
// string connectionString = System.Environment.GetEnvironmentVariable("QUEUE_CONNECTION_STRING", EnvironmentVariableTarget.Process);
string queueName = "genesis-queue-0";

// List<Order> orders = new() {
//     new Order() {OrderId = "01", Quantity = 100, UnitPrice = 9.99F},
//     new Order() {OrderId = "02", Quantity = 1000, UnitPrice = 10.282F},
//     new Order() {OrderId = "03", Quantity = 12881, UnitPrice = 8.313F},
// };

var rand = new Random();

List<InvalidOrder> invalidOrders = [
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
    new InvalidOrder() {OrdId = Guid.NewGuid().ToString(), Qty = rand.Next(0, 1000), Price = rand.NextDouble() * 1000, Desc = "This is test desc for ord"},
];

List<Order> orders = [
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100},
    new Order() {OrderId = System.Guid.NewGuid(), Quantity = rand.Next(0, 1000), UnitPrice = rand.NextSingle() * 100}
];

string[] Priority = ["Low", "Medium", "High"];

// await PeekSingleMessage();
// await PeekMessages();

// await ReceiveAndDeleteMessages();

await SendMessage(orders);
// await SendInvalidMessage(invalidOrders);
// await SendMessageWithCustomProperty(orders);

// await GetProperties();

async Task SendMessage(List<Order> orders)
{
    try
    {
        // System.Console.WriteLine(connectionString);
        ServiceBusClient serviceBusClient = new(connectionString);
        ServiceBusSender serviceBusSender = serviceBusClient.CreateSender(queueName);

        ServiceBusMessageBatch serviceBusMessageBatch = await serviceBusSender.CreateMessageBatchAsync();
        foreach (var order in orders)
        {
            Console.WriteLine(order.OrderId);
            ServiceBusMessage serviceBusMessage = new(JsonConvert.SerializeObject(order))
            {
                ContentType = "application/json"
            };
            if (!serviceBusMessageBatch.TryAddMessage(serviceBusMessage))
            {
                throw new Exception("Error occured while sneding the message to queue");
            }
        }

        Console.WriteLine($"Sending messages to Queue: {queueName}");
        await serviceBusSender.SendMessagesAsync(serviceBusMessageBatch);
        Console.WriteLine($"Successfully sent {orders.Count} messages to Queue: {queueName}");

        await serviceBusClient.DisposeAsync();
        await serviceBusSender.DisposeAsync();

    }
    catch (System.Exception)
    {
        throw;
    }
}

async Task SendInvalidMessage(List<InvalidOrder> invalidOrders)
{
    try
    {
        ServiceBusClient serviceBusClient = new(connectionString);
        ServiceBusSender serviceBusSender = serviceBusClient.CreateSender(queueName);

        ServiceBusMessageBatch serviceBusMessageBatch = await serviceBusSender.CreateMessageBatchAsync();
        foreach (var order in invalidOrders)
        {
            Console.WriteLine(order.OrdId);
            ServiceBusMessage serviceBusMessage = new(JsonConvert.SerializeObject(order))
            {
                ContentType = "application/json"
            };
            if (!serviceBusMessageBatch.TryAddMessage(serviceBusMessage))
            {
                throw new Exception("Error occured while sneding the message to queue");
            }
        }

        Console.WriteLine($"Sending messages to Queue: {queueName}");
        await serviceBusSender.SendMessagesAsync(serviceBusMessageBatch);
        Console.WriteLine($"Successfully sent {invalidOrders.Count} messages to Queue: {queueName}");

        await serviceBusClient.DisposeAsync();
        await serviceBusSender.DisposeAsync();
    }
    catch (System.Exception)
    {
        throw;
    }
}

async Task PeekMessages()
{
    ServiceBusClient serviceBusClient = new(connectionString);
    ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });

    IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();
    // ServiceBusReceivedMessage message = await serviceBusReceiver.ReceiveMessageAsync();

    await foreach (ServiceBusReceivedMessage message in messages)
    {
        var order = JsonConvert.DeserializeObject(message.Body.ToString());
        // Console.WriteLine($"Order ID: {order.OrderId}");
        // Console.WriteLine($"Order Qty: {order.Quantity}");
        // Console.WriteLine($"Order Price: {order.UnitPrice}");
        foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(order))
        {
            Console.WriteLine($"{property.Name}: {property.GetValue(order)}");
        }
        Console.WriteLine();
    }

    // Order order = JsonConvert.DeserializeObject<Order>(message.Body.ToString());
    //     Console.WriteLine($"Order ID: {order.OrderId}");
    //         Console.WriteLine($"Order Qty: {order.Quantity}");
    //         Console.WriteLine($"Order Price: {order.UnitPrice}");

    await serviceBusClient.DisposeAsync();
    await serviceBusReceiver.DisposeAsync();
}

async Task PeekSingleMessage()
{
    ServiceBusClient serviceBusClient = new(connectionString);
    ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName,
        new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock }
    );

    ServiceBusReceivedMessage message = await serviceBusReceiver.ReceiveMessageAsync();
    var order = JsonConvert.DeserializeObject(message.Body.ToString());
    foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(order))
    {
        Console.WriteLine($"{property.Name}: {property.GetValue(order)}");
    }
    System.Console.WriteLine();

    await serviceBusClient.DisposeAsync();
    await serviceBusReceiver.DisposeAsync();
}


async Task ReceiveAndDeleteMessages()
{
    ServiceBusClient serviceBusClient = new(connectionString);
    ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });

    IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();
    // ServiceBusReceivedMessage message = await serviceBusReceiver.ReceiveMessageAsync();

    await foreach (ServiceBusReceivedMessage message in messages)
    {
        var order = JsonConvert.DeserializeObject(message.Body.ToString());
        // Console.WriteLine($"Order ID: {order.OrderId}");
        // Console.WriteLine($"Order Qty: {order.Quantity}");
        // Console.WriteLine($"Order Price: {order.UnitPrice}");
        foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(order))
        {
            Console.WriteLine($"{property.Name}: {property.GetValue(order)}");
        }
        Console.WriteLine();
    }

    // Order order = JsonConvert.DeserializeObject<Order>(message.Body.ToString());
    //     Console.WriteLine($"Order ID: {order.OrderId}");
    //         Console.WriteLine($"Order Qty: {order.Quantity}");
    //         Console.WriteLine($"Order Price: {order.UnitPrice}");

    await serviceBusClient.DisposeAsync();
    await serviceBusReceiver.DisposeAsync();
}


async Task GetProperties()
{
    ServiceBusClient serviceBusClient = new(connectionString);
    ServiceBusReceiver serviceBusReceiver = serviceBusClient.CreateReceiver(queueName,
        new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock }
    );
    IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();

    await foreach (ServiceBusReceivedMessage message in messages)
    {
        Console.WriteLine($"MessageID: {message.MessageId}\t SequenceNumber: {message.SequenceNumber}\t Priority: {message.ApplicationProperties.GetValueOrDefault("Priority")}");
    }

    await serviceBusClient.DisposeAsync();
    await serviceBusReceiver.DisposeAsync();

}


async Task SendMessageWithCustomProperty(List<Order> orders)
{
    try
    {
        ServiceBusClient serviceBusClient = new(connectionString);
        ServiceBusSender serviceBusSender = serviceBusClient.CreateSender(queueName);

        ServiceBusMessageBatch serviceBusMessageBatch = await serviceBusSender.CreateMessageBatchAsync();
        foreach (var order in orders)
        {
            Console.WriteLine(order.OrderId);
            ServiceBusMessage serviceBusMessage = new(JsonConvert.SerializeObject(order))
            {
                ContentType = "application/json",
            };

            // Setting Custom Application Properties to Messages
            // Eg: Setting Priority value to messages on a random basis
            serviceBusMessage.ApplicationProperties.Add("Priority", Priority[rand.Next(0, 2)]);

            if (!serviceBusMessageBatch.TryAddMessage(serviceBusMessage))
            {
                throw new Exception("Error occured while sneding the message to queue");
            }
        }

        Console.WriteLine($"Sending messages to Queue: {queueName}");
        await serviceBusSender.SendMessagesAsync(serviceBusMessageBatch);
        Console.WriteLine($"Successfully sent {orders.Count} messages to Queue: {queueName}");

        await serviceBusClient.DisposeAsync();
        await serviceBusSender.DisposeAsync();

    }
    catch (System.Exception)
    {
        throw;
    }
}