// Receive: el CONSUMIDOR. Se conecta a RabbitMQ y se queda escuchando la cola "hello"
// hasta que lo cortemos con Ctrl+C.
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 1) Misma conexión que en Send, con otro nombre para distinguirlo en la interfaz web.
var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-receive"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 2) Declaramos la MISMA cola. ¿Por qué otra vez? Porque no sabemos si arranca
//    primero Send o Receive: el que llegue primero la crea.
await channel.QueueDeclareAsync(
    queue: "hello",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

// 3) El consumidor. El evento ReceivedAsync se dispara cada vez que el broker nos entrega
//    un mensaje: no preguntamos si hay algo, RabbitMQ nos lo empuja.
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");
    return Task.CompletedTask;
};

// 4) Nos suscribimos a la cola. autoAck: true = el broker da el mensaje por entregado apenas
//    nos lo manda. Es lo más simple, pero si el programa se cae procesándolo, el mensaje se
//    pierde. En el ejercicio 02 pasamos a confirmar a mano (ack).
await channel.BasicConsumeAsync(queue: "hello", autoAck: true, consumer: consumer);

// 5) Mantenemos vivo el programa hasta que el usuario presione Ctrl+C.
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
}
