// NewTask: el PRODUCTOR de tareas. Manda UNA tarea (el texto que le pasemos por argumentos)
// a la cola "task_queue". Cada punto "." del texto simula 1 segundo de trabajo.
//   dotnet run --project NewTask "Tarea pesada..."
using System.Text;
using RabbitMQ.Client;

// 1) Misma conexión que en el ejercicio 01, con otro nombre para reconocerla en la interfaz web.
var factory = new ConnectionFactory
{
    HostName = "localhost",
    ClientProvidedName = "tutorial-newtask"
};

using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();

// 2) Cola quorum = durable: si el broker se reinicia, las tareas pendientes NO se pierden.
//    Productor y worker la declaran con los MISMOS parámetros.
await channel.QueueDeclareAsync(
    queue: "task_queue",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

// 3) El mensaje sale de los argumentos de la línea de comandos (o "Hello World!" si no hay).
string message = args.Length > 0 ? string.Join(" ", args) : "Hello World!";
byte[] body = Encoding.UTF8.GetBytes(message);

// 4) Persistent = true: le pedimos al broker que guarde el mensaje en disco.
//    (En una cola quorum se guarda siempre; en una cola clásica durable hace falta marcarlo.)
var properties = new BasicProperties
{
    Persistent = true
};

// 5) Publicamos en el exchange por defecto con routing key "task_queue".
//    mandatory: true = si el mensaje no puede llegar a ninguna cola, el broker lo devuelve
//    en vez de descartarlo en silencio.
await channel.BasicPublishAsync(
    exchange: string.Empty,
    routingKey: "task_queue",
    mandatory: true,
    basicProperties: properties,
    body: body);

Console.WriteLine($" [x] Sent {message}");
