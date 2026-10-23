// EmitLogDirect: publica un log con una SEVERIDAD (info / warning / error) como routing key
// en el exchange "direct_logs" (tipo direct).
//   dotnet run --project EmitLogDirect error "Se cayó la base de datos"
using System.Text;
using RabbitMQ.Client;

// 1) Primer argumento = severidad (routing key). El resto = el texto del mensaje.
string severity = args.Length > 0 ? args[0] : "info";
string message = args.Length > 1 ? string.Join(" ", args.Skip(1)) : "Hello World!";

var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-emitlogdirect"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 2) direct = entrega el mensaje a las colas cuya binding key sea IGUAL a la routing key.
await channel.ExchangeDeclareAsync(exchange: "direct_logs", type: ExchangeType.Direct);

// 3) routingKey = la severidad. Es lo que el exchange compara contra las binding keys.
//    Si ninguna cola está enlazada con esa severidad, el broker descarta el mensaje en silencio.
byte[] body = Encoding.UTF8.GetBytes(message);
await channel.BasicPublishAsync(exchange: "direct_logs", routingKey: severity, body: body);

Console.WriteLine($" [x] Sent '{severity}':'{message}'");
