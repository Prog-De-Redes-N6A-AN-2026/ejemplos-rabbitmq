// EmitLogTopic: publica un log con una routing key de VARIAS PALABRAS separadas por puntos,
// con el formato "<origen>.<severidad>", en el exchange "topic_logs" (tipo topic).
//   dotnet run --project EmitLogTopic kern.critical "Error crítico del kernel"
using System.Text;
using RabbitMQ.Client;

// 1) Primer argumento = routing key (ej. "kern.critical"). El resto = el texto del mensaje.
string routingKey = args.Length > 0 ? args[0] : "anonymous.info";
string message = args.Length > 1 ? string.Join(" ", args.Skip(1)) : "Hello World!";

var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-emitlogtopic"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 2) topic = entrega a las colas cuyo PATRÓN (binding key) coincida con la routing key.
//    Los patrones pueden tener comodines: * = exactamente una palabra, # = cero o más.
await channel.ExchangeDeclareAsync(exchange: "topic_logs", type: ExchangeType.Topic);

// 3) Publicamos con la routing key de varias palabras. Si ningún patrón coincide,
//    el exchange descarta el mensaje en silencio: el productor no se entera.
byte[] body = Encoding.UTF8.GetBytes(message);
await channel.BasicPublishAsync(exchange: "topic_logs", routingKey: routingKey, body: body);

Console.WriteLine($" [x] Sent '{routingKey}':'{message}'");
