// Worker: el CONSUMIDOR de tareas. Podemos levantar varios a la vez: RabbitMQ reparte
// las tareas entre ellos. Cada worker procesa UNA tarea por vez.
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// 1) Conexión y canal, con un nombre para reconocer al worker en la interfaz web.
var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-worker"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 2) La MISMA cola que declara NewTask (mismos parámetros).
await channel.QueueDeclareAsync(
    queue: "task_queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

// 3) Prefetch = 1: "no me mandes otra tarea hasta que confirme (ack) la que estoy haciendo".
//    Así una tarea larga no hace que se acumulen otras en este worker mientras
//    otro worker está libre (despacho justo / fair dispatch).
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

// 4) El handler ahora es async: el "trabajo" lo simulamos con await Task.Delay.
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, ea) =>
{
    byte[] body = ea.Body.ToArray();
    string message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");

    await DoWorkAsync(message);

    Console.WriteLine(" [x] Done");

    // 5) Confirmamos (ack) RECIÉN al terminar. Si el worker muere a mitad de la tarea,
    //    el mensaje queda sin confirmar y RabbitMQ se lo entrega a otro worker.
    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
};

// 6) autoAck: false = los acks los mandamos nosotros (paso 5), no el broker al entregar.
await channel.BasicConsumeAsync(queue: "task_queue", autoAck: false, consumer: consumer);

// 7) Mantenemos vivo el programa hasta que el usuario presione Ctrl+C.
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

// Simula una tarea que tarda: 1 segundo por cada "." del mensaje.
static async Task DoWorkAsync(string task)
{
    int dots = task.Count(ch => ch == '.');
    await Task.Delay(dots * 1000);
}
