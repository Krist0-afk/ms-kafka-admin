# ms-kafka-admin

Microservicio de Pedidos360 que administra la plataforma Kafka con el `AdminClient`: tópicos,
configuraciones y consumer groups. Spring Boot 4.1 · Java 21 · puerto **8088**.

Al arrancar crea los tópicos declarados en `application.yml` (`app.kafka.topicos`) si no existen,
así la configuración de cada tópico queda versionada en el código:

| Tópico | Particiones × réplicas | Retención | Cleanup |
|---|---|---|---|
| `pedidos360.interacciones` | 3 × 3 | 7 días | delete |
| `pedidos360.pagos` | 3 × 3 | 30 días | delete |
| `pedidos360.interacciones.DLT` | 3 × 3 | 14 días | delete |
| `pedidos360.pagos.DLT` | 3 × 3 | 30 días | delete |

Todos con `min.insync.replicas=2`: con `acks=all` se tolera la caída de un broker sin perder mensajes.

## Endpoints (`/api/v1/kafka-admin`, rol Admin)

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/health` | Estado (público) |
| GET | `/cluster` | Brokers y controlador |
| GET | `/topics` | Lista de tópicos |
| GET | `/topics/{nombre}` | Particiones, líder, réplicas, ISR y configuración |
| POST | `/topics` | Crea un tópico (`nombre`, `particiones`, `replicas`, `configs`) |
| PUT | `/topics/{nombre}/config` | Modifica configuraciones (p. ej. `retention.ms`) |
| POST | `/topics/{nombre}/partitions` | Aumenta las particiones |
| DELETE | `/topics/{nombre}` | Elimina un tópico |
| GET | `/consumer-groups` | Grupos con estado, miembros y lag total |
| GET | `/consumer-groups/{grupo}` | Offsets confirmados, último offset y lag por partición |
| POST | `/consumer-groups/{grupo}/reset` | Mueve los offsets a `earliest` o `latest` |
| DELETE | `/consumer-groups/{grupo}` | Elimina un grupo |

## Variables de entorno

| Variable | Ejemplo |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka1:29092,kafka2:29092,kafka3:29092` |
| `COGNITO_ISSUER`, `COGNITO_CLIENT_ID` | Pool de AWS Cognito |
| `CORS_ALLOWED_ORIGINS` | `https://<ip-del-front>` |

## Comandos

```bash
./mvnw spring-boot:run
./mvnw test
docker build -t pedidos360-kafka-admin .
```

El despliegue completo está en `Backend-cloud-ordenes/deploy`.
