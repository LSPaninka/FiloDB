# 09 — Ingesta de Datos

## Visión General

FiloDB soporta múltiples mecanismos de ingestión de datos. El flujo principal es a través de **Apache Kafka**, pero también se puede usar el **Gateway** para aceptar datos en formato Prometheus Remote Write o InfluxDB Line Protocol.

---

## Mecanismos de Ingestión

```
                     ┌─────────────────────────────────────────┐
                     │         Fuentes de Datos                 │
                     │                                          │
                     │  ┌────────────┐  ┌────────────┐         │
                     │  │ Prometheus │  │  Telegraf   │         │
                     │  │ Remote     │  │  InfluxDB   │         │
                     │  │ Write      │  │  Line Proto │         │
                     │  └─────┬──────┘  └─────┬──────┘         │
                     │        │               │                 │
                     └────────┼───────────────┼─────────────────┘
                              │               │
                              ▼               ▼
                     ┌────────────────────────────────┐
                     │     Gateway (Netty TCP)         │
 Mecanismo 1:        │                                 │
 Gateway → Kafka     │  Conversión a RecordContainer   │
                     │  Particionamiento por shard     │
                     │  Publicación a Kafka             │
                     └──────────────┬─────────────────┘
                                    │
                                    ▼
                     ┌────────────────────────────────┐
                     │        Apache Kafka              │
 Mecanismo 2:        │                                 │
 Kafka directo       │  Topic por dataset              │
                     │  Partición por shard             │
                     └──────────────┬─────────────────┘
                                    │
                                    ▼
                     ┌────────────────────────────────┐
                     │      FiloDB Node                 │
                     │                                 │
                     │  KafkaIngestionStreamFactory    │
                     │  ↓                              │
                     │  IngestionActor (por shard)     │
                     │  ↓                              │
                     │  MemStore (Off-Heap)            │
                     └────────────────────────────────┘
```

---

## Mecanismo 1: Gateway (Prometheus Remote Write)

El **Gateway** es un servidor Netty de alto rendimiento que acepta datos y los publica en Kafka.

### Configuración de Prometheus para Remote Write

```yaml
# prometheus.yml
remote_write:
  - url: "http://filodb-gateway:9175/remote/write"
    queue_config:
      max_samples_per_send: 1000
      batch_send_deadline: 5s
      max_retries: 3
```

### Formatos Aceptados por el Gateway

| Formato | Protocolo | Descripción |
|---------|-----------|-------------|
| **Prometheus Remote Write** | HTTP/Protobuf | Formato nativo de Prometheus |
| **InfluxDB Line Protocol** | TCP | Formato de texto de InfluxDB |
| **Graphite** | TCP | Formato de texto de Graphite |

### Ejemplo: Envío con InfluxDB Line Protocol

```
# Formato: measurement,tag1=val1,tag2=val2 field1=value1,field2=value2 timestamp
cpu_usage,host=server1,region=us-east value=0.85 1609461000000000000
memory_free,host=server1,region=us-east bytes=4294967296 1609461000000000000
disk_io,host=server1,device=sda read_bytes=1234567,write_bytes=7654321 1609461000000000000
```

### Conversión de Formatos

El Gateway convierte los datos entrantes al formato binario `RecordContainer`:

```
┌─────────────────────────────────────────────────────────────┐
│ Datos de entrada (Prometheus Remote Write)                   │
│                                                              │
│ metric: http_requests_total                                  │
│ labels: {method="GET", handler="/api", status="200"}         │
│ value: 1234.0                                                │
│ timestamp: 1609461000                                        │
└───────────────────────────┬──────────────────────────────────┘
                            │ Gateway Converter
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ RecordContainer (binario)                                    │
│                                                              │
│ Schema: prom-counter                                         │
│ PartitionKey: hash(__name__="http_requests_total",           │
│               method="GET", handler="/api", status="200")    │
│ Timestamp: 1609461000000 (ms)                                │
│ Value: 1234.0 (double)                                       │
│ ShardKey: hash de shard tags                                 │
└───────────────────────────┬──────────────────────────────────┘
                            │ Publica en Kafka
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ Kafka Topic: timeseries-dev                                  │
│ Partición: determinada por ShardKey                          │
└─────────────────────────────────────────────────────────────┘
```

### Escritores de Esquemas del Gateway

El Gateway tiene escritores especializados para cada tipo de dato:

| Escritor | Esquema | Uso |
|----------|---------|-----|
| `writeGaugeRecord()` | gauge | Valores instantáneos |
| `writePromCounterRecord()` | prom-counter | Contadores Prometheus |
| `writeDeltaCounterRecord()` | delta-counter | Contadores delta |
| `writeOtelCumulativeHistRecord()` | otel-cumulative-histogram | Histogramas OTel acumulativos |
| `writeDeltaHistRecord()` | delta-histogram | Histogramas delta |
| `writeOtelDeltaHistRecord()` | otel-delta-histogram | Histogramas OTel delta |
| `writeOtelExpDeltaHistRecord()` | otel-exp-delta-histogram | Histogramas OTel exponenciales |

---

## Mecanismo 2: Kafka Directo

Para ingestión personalizada, puedes escribir directamente en el topic de Kafka usando el formato `RecordContainer`.

### Producir Datos en Kafka

```scala
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schema

// Crear un builder con el esquema apropiado
val builder = new RecordBuilder(schema, 4096)

// Agregar un registro
builder.startNewRecord()
builder.addLong(timestamp)        // Columna de timestamp
builder.addDouble(value)          // Columna de valor
builder.addString("metric_name")  // Label __name__
builder.addString("job_value")    // Label job
builder.endRecord()

// Obtener el RecordContainer binario
val container = builder.allContainers.head

// Publicar en Kafka
kafkaProducer.send(new ProducerRecord(
  "timeseries-dev",        // Topic
  shardNum,                // Partición = shard
  null,                    // Key
  container.bytes          // Value (RecordContainer serializado)
))
```

---

## Mecanismo 3: Ingesta Custom

FiloDB permite implementar factorías de ingestión personalizadas:

```scala
// Implementar la interfaz IngestionStreamFactory
class MyCustomIngestionStreamFactory extends IngestionStreamFactory {
  
  def create(
    config: Config,
    schema: Schema,
    shard: Int
  ): IngestionStream = {
    // Tu lógica de ingestión personalizada
    new MyCustomIngestionStream(config, schema, shard)
  }
}
```

**Configuración:**
```hocon
sourcefactory = "com.mycompany.MyCustomIngestionStreamFactory"
```

---

## Configuración de Ingestión

### Configuración Completa de Fuente de Datos

```hocon
# conf/timeseries-dev-source.conf
dataset = "prometheus"
schema = "prom-counter"
num-shards = 4
min-num-nodes = 1

# Fábrica de streams de ingestión
sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"

sourceconfig {
  # === Kafka ===
  filo-topic-name = "timeseries-dev"
  bootstrap.servers = "localhost:9092"
  group.id = "filo-db-timeseries-ingestion"
  
  # === Almacenamiento en Memoria ===
  store {
    # Intervalo de flush a Cassandra
    flush-interval = 1h
    
    # TTL de datos en disco (Cassandra)
    disk-time-to-live = 24 hours
    
    # Memoria off-heap por shard
    shard-mem-size = 256MB
    
    # Memoria para buffers de ingesta
    ingestion-buffer-mem-size = 200MB
    
    # Grupos por shard
    # Cada grupo se flushea independientemente
    groups-per-shard = 20
    
    # Número de buffer pools para ingesta
    num-buffer-pools = 10
    
    # Tamaño máximo de chunk (número de muestras)
    max-chunks-size = 400
    
    # Paralelismo de demand-paging (carga desde Cassandra)
    demand-paging-parallelism = 4
    
    # Tiempo mínimo que los datos se mantienen en memoria
    min-time-retention-hours = 12
  }
  
  # === Downsampling ===
  downsample {
    resolutions = [1, 5]     # minutos
    ttls = [30, 183]         # días
    raw-schema-changes = []
  }
}
```

---

## Esquemas de Datos Soportados

FiloDB soporta múltiples esquemas para diferentes tipos de métricas:

### Esquemas Básicos

| Esquema | Columnas | Uso |
|---------|----------|-----|
| `gauge` | `timestamp:ts, value:double` | Valores instantáneos (temperatura, memoria libre) |
| `untyped` | `timestamp:ts, value:double` | Tipo sin especificar |
| `prom-counter` | `timestamp:ts, value:double:detectDrops=true` | Contadores monótonos (requests totales) |
| `prom-histogram` | `timestamp:ts, sum:long, count:long, h:hist` | Histogramas Prometheus nativos |

### Esquemas Delta

| Esquema | Columnas | Uso |
|---------|----------|-----|
| `delta-counter` | `timestamp:ts, value:double` | Contadores que reportan deltas |
| `delta-histogram` | `timestamp:ts, sum:long, count:long, h:hist:delta=true` | Histogramas con valores delta |

### Esquemas OpenTelemetry

| Esquema | Columnas | Uso |
|---------|----------|-----|
| `otel-cumulative-histogram` | `timestamp:ts, sum:double, count:double, h:hist:counter=true, min:double, max:double` | Histogramas OTel acumulativos |
| `otel-delta-histogram` | `timestamp:ts, sum:double, count:double, h:hist:delta=true, min:double, max:double` | Histogramas OTel delta |
| `otel-exp-delta-histogram` | `timestamp:ts, sum:double, count:double, h:hist:exp=true:delta=true, min:double, max:double` | Histogramas OTel exponenciales |

### Esquemas Pre-agregados

| Esquema | Uso |
|---------|-----|
| `preagg-gauge` | Gauge pre-agregado |
| `preagg-delta-counter` | Counter delta pre-agregado |
| `preagg-otel-delta-histogram` | Histograma OTel delta pre-agregado |
| `preagg-otel-exp-delta-histogram` | Histograma OTel exponencial pre-agregado |

---

## Particiones y Shard Keys

### Cómo se Particionan los Datos

Los datos se distribuyen entre shards basándose en las **shard keys** (etiquetas de partición):

```
Métrica entrante:
  __name__ = "http_requests_total"
  job = "web_server"
  instance = "server1:9090"
  method = "GET"
  status = "200"

Partition Key = hash(todas las etiquetas)
Shard Key = hash(__name__, job)  # Solo las etiquetas de shard

Shard asignado = Shard Key % num_shards
```

### Configuración de Partition Keys

```hocon
# Las partition key labels se definen en el esquema
partition-schema {
  columns = ["_metric_:string", "_ws_:string", "_ns_:string"]
  predefined-keys = ["_metric_", "_ws_", "_ns_"]
  
  # Labels que determinan el shard
  options {
    shardKeyColumns = ["_metric_", "_ws_"]
    ignoreShardKeyColumnSuffixes = {"_metric_" = ["_total", "_sum", "_count", "_bucket"]}
    ignoreTagsOnPartitionKeyHash = ["le"]
  }
}
```

---

## Monitoreo de la Ingesta

### Métricas HTTP
```bash
# Estado de los shards incluyendo particiones activas
curl http://localhost:8080/api/v1/cluster/prometheus/status
```

### Métricas Internas de FiloDB

| Métrica | Descripción |
|---------|-------------|
| `memstore_rows_ingested_total` | Total de filas ingeridas |
| `memstore_rows_ingested_bytes` | Bytes ingeridos |
| `memstore_partitions_active` | Particiones activas en memoria |
| `memstore_chunks_flushed_total` | Chunks flushed a Cassandra |
| `kafka_consumer_offset_lag` | Lag del consumidor de Kafka |
| `ingestion_errors_total` | Errores de ingestión |

---

## Consideraciones de Memoria

### Dimensionamiento de Memoria

```
Memoria total por nodo = (shard-mem-size + ingestion-buffer-mem-size) × num_shards_por_nodo

Ejemplo con 4 shards por nodo:
  (256MB + 200MB) × 4 = 1.824 GB de memoria off-heap
  + ~1-2 GB para heap de JVM
  = ~4 GB mínimo por nodo
```

### Recomendaciones

| Escenario | shard-mem-size | ingestion-buffer | Shards/nodo |
|-----------|---------------|-------------------|-------------|
| **Desarrollo** | 256MB | 200MB | 4 |
| **Producción pequeña** | 512MB | 400MB | 4-8 |
| **Producción grande** | 1GB | 800MB | 8-16 |
| **Alta cardinalidad** | 2GB | 1.5GB | 4-8 |
