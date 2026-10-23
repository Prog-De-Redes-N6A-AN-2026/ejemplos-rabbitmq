// ReceiveLogs: un SUSCRIPTOR. Cada instancia crea su propia cola temporal, la engancha
// al exchange "logs" y recibe una copia de TODOS los mensajes, hasta que la cortemos con Ctrl+C.
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-receivelogs"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 1) Declaramos el mismo exchange (por si el suscriptor arranca antes que el productor).
await channel.ExchangeDeclareAsync(exchange: "logs", type: ExchangeType.Fanout);

// 2) Cola TEMPORAL: QueueDeclareAsync() sin argumentos = sin nombre (el broker inventa uno,
//    tipo "amq.gen-..."), exclusive = solo esta conexión la puede usar, autoDelete = se borra
//    sola cuando nos desconectamos.
QueueDeclareOk queueDeclareResult = await channel.QueueDeclareAsync();
string queueName = queueDeclareResult.QueueName;
Console.WriteLine($" [*] My temporary queue is {queueName}");

// 3) BINDING: "exchange logs, mandá copias a mi cola". La routing key se ignora en fanout.
await channel.QueueBindAsync(queue: queueName, exchange: "logs", routingKey: string.Empty);

Console.WriteLine(" [*] Waiting for logs. To exit press CTRL+C");

// 4) Consumimos de NUESTRA cola, igual que en el ejercicio 01.
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] {message}");
    return Task.CompletedTask;
};

// 5) autoAck: true, como en el tutorial: para logs alcanza, si se pierde uno no es grave.
//    (Los ack manuales los vimos en el ejercicio 02.)
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
    // Ctrl+C: salimos del Delay y los "using" cierran canal y conexión.
    // Al cerrarse la conexión, el broker borra nuestra cola temporal (y su binding).
}
