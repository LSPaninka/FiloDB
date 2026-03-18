# 04 — Integración con Apache Kafka

## Visión General

Apache Kafka es el **sistema de ingestión primario** de FiloDB. Los datos de series temporales llegan a topics de Kafka en formato binario (`RecordContainer`) y son consumidos por los nodos del cluster de FiloDB.

```
┌───────────────┐     ┌───────────────┐     ┌──────────────────────┐
│  Prometheus    │     │  Gateway      │     │  FiloDB Node         │
│  Remote Write  │────▶│  (Netty)      │────▶│                      │
└───────────────┘     │  Convierte a  │     │  KafkaIngestion      │
                      │  RecordContainer    │  StreamFactory       │
┌───────────────┐     │  y publica    │     │                      │
│  Telegraf      │────▶│  en Kafka     │     │  ┌──────────────┐   │
│  InfluxDB Line │     └───────┬───────┘     │  │ IngestionActor│   │
└───────────────┘             │              │  │  (por shard)  │   │
                              ▼              │  └──────┬───────┘   │
                      ┌───────────────┐      │         │           │
                      │  Apache Kafka │      │         ▼           │
                      │               │◀─────│  Consume mensajes   │
                      │  Topic:       │      │  RecordContainer    │
                      │  timeseries-  │      │         │           │
                      │  dev          │      │         ▼           │
                      │               │      │  ┌──────────────┐   │
                      │  Particiones: │      │  │  MemStore     │   │
                      │  0, 1, 2, 3   │      │  │  (Off-Heap)   │   │
                      └───────────────┘      │  └──────────────┘   │
                                             └──────────────────────┘
```

---

## Módulo `kafka/`

### Clase Principal: `KafkaIngestionStreamFactory`

Esta es la fábrica que crea streams de consumo de Kafka para cada shard de FiloDB.

**Ubicación:** `kafka/src/main/scala/filodb/kafka/KafkaIngestionStreamFactory.scala`

**Funcionamiento:**
1. Se instancia una vez por dataset en cada nodo
2. Crea un consumer de Kafka por cada shard asignado al nodo
3. Cada consumer lee de una partición de Kafka específica
4. Los mensajes se deserializan como `RecordContainer`
5. Los registros se alimentan al `IngestionActor` del shard correspondiente

### Deserialización: `RecordContainerDeserializer`

Los mensajes de Kafka deben usar el formato `RecordContainer`:
- Formato binario compacto
- Cada mensaje contiene múltiples registros de series temporales
- El esquema (RecordSchema) debe ser consistente dentro de un topic
- **Un topic por dataset**

---

## Configuración de Kafka

### Configuración de Fuente de Datos

**Archivo:** `conf/timeseries-dev-source.conf`

```hocon
dataset = "prometheus"
schema = "prom-counter"
num-shards = 4
min-num-nodes = 1

sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"

sourceconfig {
  # Topic de Kafka a consumir
  filo-topic-name = "timeseries-dev"
  
  # Conexión a Kafka
  bootstrap.servers = "localhost:9092"
  group.id = "filo-db-timeseries-ingestion"
  
  # Configuración de consumo
  record.converter = "filodb.gateway.convertors.PrometheusInputRecordConverter"
  
  # Almacenamiento en memoria
  store {
    # Intervalo de flush a Cassandra
    flush-interval = 1h
    
    # TTL de datos en disco
    disk-time-to-live = 24 hours
    
    # Memoria por shard
    shard-mem-size = 256MB
    ingestion-buffer-mem-size = 200MB
    
    # Grupos por shard (afecta el flush)
    groups-per-shard = 20
    
    # Número de buffers de ingesta
    num-buffer-pools = 10
    
    # Tamaño máximo de chunk
    max-chunks-size = 400
    
    # Demanda por defecto
    demand-paging-parallelism = 4
    
    # Tiempo mínimo de retención en memoria
    min-time-retention-hours = 12
  }

  # Downsampling
  downsample {
    # Resoluciones y TTLs
    resolutions = [1, 5]  # minutos
    ttls = [30, 183]      # días
    
    raw-schema-changes = []
  }
}
```

### Configuración de Kafka Avanzada

```hocon
sourceconfig {
  # Configuración del consumer de Kafka
  bootstrap.servers = "kafka1:9092,kafka2:9092,kafka3:9092"
  group.id = "filodb-ingestion"
  
  # Offsets
  auto.offset.reset = "latest"
  enable.auto.commit = false  # FiloDB gestiona offsets manualmente
  
  # Performance
  fetch.min.bytes = 1
  fetch.max.wait.ms = 500
  max.partition.fetch.bytes = 1048576
  
  # Timeouts
  session.timeout.ms = 30000
  heartbeat.interval.ms = 10000
  
  # Topic
  filo-topic-name = "metrics-production"
}
```

---

## Relación Shards ↔ Particiones de Kafka

FiloDB establece una relación **1:1** entre shards de FiloDB y particiones de Kafka:

```
Kafka Topic: timeseries-dev (4 particiones)
├── Partición 0  →  FiloDB Shard 0
├── Partición 1  →  FiloDB Shard 1
├── Partición 2  →  FiloDB Shard 2
└── Partición 3  →  FiloDB Shard 3
```

**Importante:**
- El número de particiones de Kafka debe coincidir con `num-shards`
- Si tienes 4 shards, necesitas 4 particiones en el topic
- El Gateway se encarga de enrutar los datos al shard correcto basándose en el hash de las claves de partición

---

## Formato de Mensajes: RecordContainer

Cada mensaje de Kafka contiene un `RecordContainer` binario:

```
┌─────────────────────────────────────────────────┐
│ RecordContainer                                  │
├─────────────────────────────────────────────────┤
│ Header:                                          │
│   - Schema ID                                    │
│   - Número de registros                          │
│   - Longitud total en bytes                      │
├─────────────────────────────────────────────────┤
│ Registro 1:                                      │
│   - Partition Key (tags/labels hash)             │
│   - Timestamp                                    │
│   - Value(s) (double, long, histogram, etc.)     │
├─────────────────────────────────────────────────┤
│ Registro 2:                                      │
│   - ...                                          │
├─────────────────────────────────────────────────┤
│ ...                                              │
└─────────────────────────────────────────────────┘
```

**Características del formato:**
- Binario compacto — sin overhead de JSON/texto
- Batching — múltiples registros por mensaje
- Schema-aware — cada container referencia su esquema
- Zero-copy — puede leerse directamente de buffers off-heap

---

## Checkpointing de Offsets

FiloDB gestiona los offsets de Kafka manualmente (no usa auto-commit):

```
┌────────────────────────────────────────────────────┐
│ Cassandra: admin.checkpoints                       │
├──────────┬──────────┬───────┬───────┬──────────────┤
│ database │ dataset  │ shard │ group │ offset       │
├──────────┼──────────┼───────┼───────┼──────────────┤
│ filodb   │ prometh. │ 0     │ 0     │ 1234567      │
│ filodb   │ prometh. │ 0     │ 1     │ 1234568      │
│ filodb   │ prometh. │ 1     │ 0     │ 2345678      │
│ ...      │ ...      │ ...   │ ...   │ ...          │
└──────────┴──────────┴───────┴───────┴──────────────┘
```

**Flujo de checkpointing:**
1. El `IngestionActor` consume mensajes de Kafka
2. Cada N mensajes o cada intervalo de tiempo, se hace checkpoint
3. El offset se persiste en la tabla `checkpoints` de Cassandra
4. En caso de reinicio, se recupera desde el último offset guardado

---

## Recuperación tras Fallos

Cuando un nodo de FiloDB se reinicia o un shard se reasigna:

1. **Lee el último checkpoint** de Cassandra para el shard
2. **Recupera chunks** de Cassandra que estaban en memoria
3. **Reanuda consumo** de Kafka desde el último offset + 1
4. **Datos no persistidos** se re-consumen de Kafka (ventana de flush)

```
Línea de tiempo:
─────────────────────────────────────────────────────►
│                    │                │               │
└── Último flush     └── Último       └── Fallo       └── Reinicio
    (Cassandra)         checkpoint       del nodo         y recuperación
                        (Kafka offset)
                        
    Los datos entre el último flush y el checkpoint
    se re-consumen de Kafka automáticamente.
```

---

## Creación del Topic de Kafka

```bash
# Crear topic con 4 particiones (debe coincidir con num-shards)
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic timeseries-dev

# Verificar el topic
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic timeseries-dev
```

---

## Monitoreo de la Ingesta Kafka

### Via HTTP API
```bash
# Ver estado de los shards (incluyendo offsets de Kafka)
curl http://localhost:8080/api/v1/cluster/prometheus/status
```

### Via CLI
```bash
./filo-cli --command list --host localhost --port 8080
```

### Métricas de Ingesta

FiloDB expone métricas sobre el consumo de Kafka:
- `ingestion_records_total` — Total de registros ingeridos
- `ingestion_errors_total` — Errores de ingestión
- `kafka_consumer_lag` — Lag del consumidor de Kafka
- `shard_ingestion_rate` — Tasa de ingestión por shard
