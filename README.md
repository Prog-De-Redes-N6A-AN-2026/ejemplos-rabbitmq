# Ejemplos de RabbitMQ

Ejercicios resueltos de **RabbitMQ** con **C# (.NET 10)** y el protocolo **AMQP 0-9-1**, usando
`RabbitMQ.Client` 7.2.2. Están basados en los [tutoriales oficiales de RabbitMQ](https://www.rabbitmq.com/tutorials).

Cada carpeta tiene dos proyectos de consola: un **productor** y un **consumidor**.

| Carpeta | Proyectos | Qué hace |
|---|---|---|
| `01-hello-world` | `Send` / `Receive` | `Send` publica un mensaje en la cola `hello` (a través del exchange por defecto) y `Receive` queda escuchando esa cola e imprime lo que llega. |
| `02-work-queues` | `NewTask` / `Worker` | `NewTask` encola tareas persistentes en `task_queue`; cada `.` del texto simula 1 segundo de trabajo. Varios `Worker` se reparten las tareas, con `prefetch = 1` y confirmación manual (ack) al terminar. |
| `03-publish-subscribe` | `EmitLog` / `ReceiveLogs` | `EmitLog` publica en el exchange `logs` de tipo `fanout`. Cada `ReceiveLogs` crea una cola temporal, la enlaza al exchange y recibe una copia de todos los mensajes. |
| `04-routing` | `EmitLogDirect` / `ReceiveLogsDirect` | Exchange `direct_logs` de tipo `direct`: los mensajes se publican con una severidad (`info`, `warning`, `error`) como routing key y cada receptor solo recibe las severidades a las que se suscribió. |
| `05-topics` | `EmitLogTopic` / `ReceiveLogsTopic` | Exchange `topic_logs` de tipo `topic`: routing keys de la forma `<origen>.<severidad>` y receptores que se suscriben con patrones (`*` = una palabra, `#` = cero o más). |

## Requisitos

- [.NET SDK 10](https://dotnet.microsoft.com/download) (con .NET 8 o 9 alcanza con cambiar el `TargetFramework` de cada `.csproj`).
- Un RabbitMQ 4 corriendo en `localhost:5672`. Con Docker:

  ```bash
  docker compose up -d
  ```

  La interfaz web queda en <http://localhost:15672> (usuario `guest`, contraseña `guest`).

## Cómo ejecutarlos

Desde la carpeta de cada ejercicio, el consumidor en una terminal y el productor en otra:

```bash
# 01
dotnet run --project Receive
dotnet run --project Send

# 02 (levantar dos o más workers)
dotnet run --project Worker
dotnet run --project NewTask "Tarea pesada..."

# 03
dotnet run --project ReceiveLogs
dotnet run --project EmitLog "info: algo pasó"

# 04
dotnet run --project ReceiveLogsDirect warning error
dotnet run --project EmitLogDirect error "Se cayó la base de datos"

# 05
dotnet run --project ReceiveLogsTopic "kern.*" "*.critical"
dotnet run --project EmitLogTopic kern.critical "Error crítico del kernel"
```

Los consumidores se cortan con **Ctrl+C**.
