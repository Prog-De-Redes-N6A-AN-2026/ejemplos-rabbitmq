// Send: el PRODUCTOR. Se conecta a RabbitMQ, manda UN mensaje a la cola "hello" y termina.
using System.Text;
using RabbitMQ.Client;

// 1) Dónde está el broker. Usuario, contraseña, puerto y virtual host tienen valores por
//    defecto (guest / guest / 5672 / "/"); los escribimos para que se vea qué se configura.
//    ClientProvidedName es un nombre para identificarnos en la interfaz web (pestaña "Connections").
var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    ClientProvidedName = "tutorial-send"
};

// 2) La conexión es la conexión TCP real al puerto 5672. El canal es una conversación
//    lógica dentro de esa conexión: casi todo se hace a través del canal.
//    "using" cierra canal y conexión al terminar, aunque haya un error.
using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 3) Declaramos la cola "hello". Es idempotente: si ya existe con la misma configuración,
//    no pasa nada. x-queue-type = quorum: cola durable y replicada (la recomendada en RabbitMQ 4).
await channel.QueueDeclareAsync(
    queue: "hello",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

// 4) Un mensaje es un arreglo de bytes: convertimos el texto a UTF-8.
const string message = "Hello World!";
byte[] body = Encoding.UTF8.GetBytes(message);

// 5) Publicamos en el exchange por defecto (nombre vacío). Ese exchange entrega el mensaje
//    en la cola cuyo nombre coincide con la routing key: en este caso, "hello".
await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "hello", body: body);

Console.WriteLine($" [x] Sent {message}");
