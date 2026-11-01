# Kafka – Pedidos360

## 1. Descripción

Pedidos360 implementa procesamiento de eventos y streaming de datos mediante Apache Kafka como complemento a la arquitectura existente.

Esta integración permite el manejo de flujos de datos de alto rendimiento, asegurando resiliencia, trazabilidad de eventos y coexistencia pacífica con el ecosistema de RabbitMQ.

## 2. Arquitectura

La integración incluye los siguientes componentes:

- **Pedidos Service:** publica eventos de creación de pedidos y consume los eventos de registro de usuarios utilizando el grupo `pedidos-service-group`.
- **Usuarios Service:** publica eventos de registro de nuevos usuarios y consume los eventos de creación de pedidos utilizando el grupo `usuarios-service-group`.
- **Kafka Admin Service:** microservicio dedicado que permite administrar, configurar y consultar tópicos y grupos de consumidores (*consumer groups*).
- **Kafka & Zookeeper:** clúster intermediario encargado de almacenar y distribuir los flujos de eventos.

### Flujo de comunicación

1. Se publica un evento (ej. `PedidoCreadoEvent` o `UsuarioRegistradoEvent`) a través de `KafkaTemplate`.
2. El broker de Kafka recibe el evento y lo almacena de forma inmutable en el tópico correspondiente, utilizando el ID del evento como llave (*key*).
3. Los consumidores, aislados por su `groupId`, leen los mensajes a su propio ritmo.
4. Pedidos Service y Usuarios Service procesan el evento delegando la lógica a servicios de dominio independientes.
5. Si el procesamiento es exitoso, se avanza el *offset* del consumidor.
6. Si ocurre un error, el sistema aplica una política de reintentos (*backoff* exponencial). Si los reintentos se agotan o el *payload* es corrupto, el mensaje se deriva a un Dead Letter Topic (DLT).

## 3. Configuración de Kafka

La infraestructura de Kafka se ejecuta localmente mediante Docker Compose.

- Puerto Broker Kafka: `9092` (externo) / `29092` (interno)
- Puerto Zookeeper: `2181`
- Interfaz gráfica (Kafka-UI): `http://localhost:8085`
- Serialización: `StringSerializer` (Key) y `JacksonJsonSerializer` (Value).

### Tópicos utilizados

| Tópico | Función |
|---|---|
| `pedidos360.pedidos.creados` | Eventos de creación de pedidos emitidos por Pedidos Service |
| `pedidos360.pedidos.creados.DLT` | Mensajes fallidos del tópico de pedidos tras agotar reintentos |
| `pedidos360.usuarios.registrados` | Eventos de registro emitidos por Usuarios Service |
| `pedidos360.usuarios.registrados.DLT` | Mensajes fallidos del tópico de usuarios tras agotar reintentos |

## 4. Manejo de mensajes

### Grupos de Consumidores (Consumer Groups)

Se utilizan `groupId` explícitos (`pedidos-service-group` y `usuarios-service-group`) configurados en la anotación `@KafkaListener`. Esto permite balanceo de carga y procesamiento paralelo (concurrencia de 3 hilos) si se levantan múltiples instancias de un mismo microservicio.

### Reintentos (Retries)

El manejo de fallos temporales está gestionado por un `DefaultErrorHandler`. Por defecto, realiza 3 reintentos con un *backoff* exponencial (espera inicial de 1s, multiplicador x2, máximo 5s). 

### DLT (Dead Letter Topic)

Los eventos que agotan los reintentos permitidos, o aquellos con *payloads* corruptos capturados por `ErrorHandlingDeserializer`, son enrutados automáticamente al tópico con sufijo `.DLT` para evitar bloquear a los consumidores y permitir su análisis.

## 5. Administración de Kafka

El microservicio Kafka Admin (puerto `8083`) permite realizar operaciones administrativas avanzadas sobre el clúster mediante la clase `AdminClient`.

Entre sus funcionalidades se encuentran:

- **Tópicos:** Crear (configurando particiones, retención y *cleanup policy*), listar, ver detalle (líder, ISR, réplicas), modificar (aumentar particiones, cambiar retención) y eliminar tópicos.
- **Grupos de Consumidores:** Listar grupos, ver detalles (miembros, offsets y *lag*), inspeccionar offsets específicos, reiniciar offsets (*earliest*/*latest*) y eliminar grupos inactivos.

## 6. Pruebas realizadas

### Prueba de procesamiento exitoso

Se publicó un evento válido en formato JSON desde Postman (ej. `PedidoCreadoEvent`).

**Resultado:** El evento se registró correctamente en Kafka, los logs demostraron el consumo instantáneo por el *Listener* correspondiente, y Kafka-UI confirmó el movimiento de los *offsets*.

### Prueba de error y resiliencia

Se simuló un error de procesamiento y el envío de un JSON erroneo.

**Resultado:** El evento fue desviado exitosamente al tópico `.DLT`, protegiendo la estabilidad del consumidor.

### Integración de Microservicios

Ambos servicios (Pedidos y Usuarios) compilaron y ejecutaron sus pruebas sin conflictos con la configuración de RabbitMQ preexistente.

## 7. Tecnologías utilizadas

- Java 21
- Spring Boot 
- Spring Kafka
- Apache Kafka & Zookeeper
- Kafka-UI
- Docker Compose
- Maven

## 8. Conclusión

La integración de Kafka complementa sólidamente la arquitectura de Pedidos360, incorporando un modelo robusto de retención de eventos y alto rendimiento.

El uso de `groupId` específicos, serializadores tolerantes a fallos y políticas de reintento hacia *Dead Letter Topics* asegura que los microservicios procesen la información de manera confiable. Además, el Kafka Admin Service otorga el control total sobre la configuración avanzada de los *brokers*, cumpliendo con los estándares de gestión de plataformas *Cloud Native*.