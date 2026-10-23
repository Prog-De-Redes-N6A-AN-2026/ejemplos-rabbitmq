// EmitLog: el PRODUCTOR de logs. Publica en el exchange "logs" (tipo fanout),
// que copia el mensaje a TODAS las colas enlazadas a él.
//   dotnet run --project EmitLog "info: algo pasó"
using System.Text;
using RabbitMQ.Client;

// 1) El mensaje sale de los argumentos de la línea de comandos (o "info: Hello World!" si no hay).
string message = args.Length > 0 ? string.Join(" ", args) : "info: Hello World!";

// 2) Misma conexión que en los ejercicios anteriores: conexión TCP + un canal adentro.
var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-emitlog"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 3) Ahora declaramos un EXCHANGE, no una cola. El productor ya no sabe nada de colas.
//    fanout = "repartir a todos": ignora la routing key y copia el mensaje a cada cola enlazada.
//    Igual que con las colas, declarar es idempotente.
await channel.ExchangeDeclareAsync(exchange: "logs", type: ExchangeType.Fanout);

// 4) Publicamos en el exchange "logs" (antes usábamos el exchange por defecto, "").
//    La routing key va vacía porque fanout la ignora.
//    OJO: si no hay NINGUNA cola enlazada, el exchange no tiene a quién entregarlo y el
//    mensaje se DESCARTA en silencio: el broker no avisa nada y acá igual se imprime "Sent".
byte[] body = Encoding.UTF8.GetBytes(message);
await channel.BasicPublishAsync(exchange: "logs", routingKey: string.Empty, body: body);

Console.WriteLine($" [x] Sent {message}");
