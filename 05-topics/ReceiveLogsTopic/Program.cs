// ReceiveLogsTopic: recibe los logs cuya routing key coincida con alguno de los PATRONES.
//   * = exactamente una palabra     # = cero o más palabras
//   dotnet run --project ReceiveLogsTopic "kern.*" "*.critical"
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

if (args.Length < 1)
{
    Console.Error.WriteLine("Usage: ReceiveLogsTopic [binding_key]...");
    Environment.ExitCode = 1;
    return;
}

var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-receivelogstopic"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 1) Declaramos el mismo exchange topic (por si arrancamos antes que el productor).
await channel.ExchangeDeclareAsync(exchange: "topic_logs", type: ExchangeType.Topic);

// 2) Cola temporal, igual que en el ejercicio 03: sin argumentos = nombre inventado por
//    el broker, exclusiva de esta conexión y se borra sola al desconectarnos.
QueueDeclareOk queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;

// 3) UN binding por cada patrón. La binding key ahora puede tener comodines * y #.
//    Los patrones los interpreta el BROKER, no nuestro programa.
foreach (string bindingKey in args)
{
    await channel.QueueBindAsync(queue: queueName, exchange: "topic_logs", routingKey: bindingKey);
}

Console.WriteLine($" [*] Listening for: {string.Join(", ", args)}. To exit press CTRL+C");

// 4) ea.RoutingKey = la routing key con la que se PUBLICÓ el mensaje (ej. "kern.critical"),
//    no el patrón que coincidió.
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    string routingKey = ea.RoutingKey;
    Console.WriteLine($" [x] Received '{routingKey}':'{message}'");
    return Task.CompletedTask;
};

// 5) autoAck: true, como en el 03 y el 04. (La confirmación manual, ack, la vimos en el ejercicio 02.)
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
