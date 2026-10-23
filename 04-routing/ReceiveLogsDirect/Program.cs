// ReceiveLogsDirect: recibe SOLO los logs de las severidades que le pasemos.
//   dotnet run --project ReceiveLogsDirect warning error
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

if (args.Length < 1)
{
    Console.Error.WriteLine("Usage: ReceiveLogsDirect [info] [warning] [error]");
    Environment.ExitCode = 1;
    return;
}

var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-receivelogsdirect"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 1) Declaramos el mismo exchange (por si arrancamos antes que el productor).
await channel.ExchangeDeclareAsync(exchange: "direct_logs", type: ExchangeType.Direct);

// 2) Cola temporal, igual que en el ejercicio 03: sin argumentos = nombre inventado por
//    el broker, exclusiva de esta conexión y se borra sola al desconectarnos.
QueueDeclareOk queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;

// 3) UN binding por cada severidad pedida. Una misma cola puede tener varios bindings.
//    Acá el parámetro routingKey es la BINDING KEY: la clave que el exchange va a comparar.
foreach (string severity in args)
{
    await channel.QueueBindAsync(queue: queueName, exchange: "direct_logs", routingKey: severity);
}

Console.WriteLine($" [*] Listening for: {string.Join(", ", args)}. To exit press CTRL+C");

// 4) ea.RoutingKey = la routing key con la que se PUBLICÓ el mensaje. Viene en los
//    metadatos de la entrega, así el consumidor sabe qué severidad tenía.
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    string routingKey = ea.RoutingKey;
    Console.WriteLine($" [x] Received '{routingKey}':'{message}'");
    return Task.CompletedTask;
};

// 5) autoAck: true: son logs, perder uno si nos caemos no es grave.
//    (La confirmación manual, ack, la vimos en el ejercicio 02.)
await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

// 6) Mantenemos vivo el programa hasta que el usuario presione Ctrl+C.
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // no matar el proceso de golpe: dejamos que cierre ordenadamente
    cts.Cancel();
};

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    // Ctrl+C: salimos del Delay y los "using" cierran canal y conexión (la cola temporal se borra).
}
