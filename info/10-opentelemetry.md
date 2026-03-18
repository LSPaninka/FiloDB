# 10 — Integración con OpenTelemetry

## Visión General

FiloDB tiene una integración bidireccional con **OpenTelemetry (OTel)**:

1. **Instrumentación interna**: FiloDB usa el SDK de OpenTelemetry para emitir sus propias métricas de rendimiento
2. **Almacenamiento de datos OTel**: FiloDB define esquemas específicos para almacenar datos en formatos OpenTelemetry (histogramas acumulativos, delta, exponenciales)

---

## 1. Instrumentación Interna de FiloDB con OTel

### Configuración

FiloDB puede emitir sus métricas internas usando OpenTelemetry o Kamon (o ambos):

```hocon
# core/src/main/resources/filodb-defaults.conf
metrics {
  # Activar/desactivar instrumentación
  otel-enabled = false    # OpenTelemetry
  kamon-enabled = true    # Kamon (alternativa)
  
  otel {
    # Intervalo de exportación de métricas
    export-interval-seconds = 60
    
    # Atributos del recurso OTel
    resource-attributes {
      "service.name" = "filodb"
      "service.version" = "0.9"
      "deployment.environment" = "production"
    }
    
    # Fábrica del exportador (implementación)
    exporter-factory-class-name = "filodb.core.metrics.LogMetricExporterFactory"
    
    # === Para exportar via OTLP (gRPC/HTTP) ===
    # exporter-factory-class-name = "filodb.core.metrics.OtlpGrpcMetricExporterFactory"
    # otlp-endpoint = "http://otel-collector:4318/v1/metrics"
    # otlp-headers = { "Authorization" = "Bearer token123" }
    
    # === mTLS (para conexiones seguras) ===
    # mtls-cert-path = "/path/to/client.crt"
    # mtls-key-path = "/path/to/client.key"
    # mtls-ca-path = "/path/to/ca.crt"
    
    # Histogramas exponenciales (más eficientes)
    exponential-histogram = false
    
    # Buckets personalizados para histogramas no-exponenciales
    custom-histogram-buckets = [
      0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
      0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0
    ]
  }
}
```

### Exportadores Disponibles

| Exportador | Clase | Destino |
|------------|-------|---------|
| **Log** | `LogMetricExporterFactory` | Salida a logs (desarrollo) |
| **OTLP gRPC** | `OtlpGrpcMetricExporterFactory` | Collector OTel via gRPC |

### Métricas Emitidas por FiloDB

FiloDB instrumenta las siguientes áreas con OpenTelemetry:

#### Métricas de JVM Runtime
- **Clases**: Clases cargadas/descargadas
- **CPU**: Uso de CPU del proceso y del sistema
- **GC**: Colecciones de garbage collector, tiempos de pausa
- **Memory Pools**: Uso de heap, non-heap, metaspace
- **Threads**: Threads activos, daemon, estados

#### Métricas de Sistema (OSHI)
- CPU del sistema, carga promedio
- Memoria física, swap
- Disco: lectura/escritura por dispositivo
- Red: bytes recibidos/enviados por interfaz

#### Métricas Custom de FiloDB
- Contadores de ingestión
- Latencias de consulta
- Estado de shards
- Uso de memoria off-heap
- Flush a Cassandra

### Tipos de Métricas OTel Usados

```scala
// Contador (sólo incrementa)
val ingestedRecords: MetricsCounter = new MetricsCounter(
  meter, "filodb.ingested.records",
  "Total records ingested",
  baseAttributes
)

// UpDownCounter (puede incrementar y decrementar)
val activePartitions: MetricsUpDownCounter = new MetricsUpDownCounter(
  meter, "filodb.active.partitions",
  "Currently active partitions",
  baseAttributes
)

// Gauge (valor instantáneo)
val memoryUsage: MetricsGauge = new MetricsGauge(
  meter, "filodb.memory.used.bytes",
  "Off-heap memory usage",
  baseAttributes
)
```

### Instrumentación de ExecutorService

FiloDB también instrumenta sus thread pools con OTel:

```scala
// InstrumentedExecutorService envuelve un ExecutorService
// y emite métricas de:
// - Tareas enviadas
// - Tareas completadas
// - Tareas rechazadas
// - Tiempos de ejecución
// - Tamaño del pool
val instrumentedPool = new InstrumentedExecutorService(
  originalPool, openTelemetry, "filodb-query-pool"
)
```

---

## 2. Almacenamiento de Datos OpenTelemetry

FiloDB define esquemas específicos para almacenar métricas en formatos OpenTelemetry, particularmente histogramas que son el tipo de dato más complejo.

### Esquemas OTel Soportados

#### `otel-cumulative-histogram`

Para histogramas OTel con temporalidad acumulativa (el valor incluye todo el historial desde el inicio).

```hocon
otel-cumulative-histogram {
  columns = [
    "timestamp:ts",
    "sum:double:detectDrops=true",
    "count:double:detectDrops=true", 
    "h:hist:counter=true",
    "min:double:detectDrops=true",
    "max:double:detectDrops=true"
  ]
  
  value-column = "h"
  
  downsamplers = [
    "tTime(0)",    # Timestamp
    "dLast(1)",    # Último valor de sum
    "dLast(2)",    # Último valor de count
    "dLast(3)",    # Último histograma
    "dMin(4)",     # Mínimo del período
    "dMax(5)"      # Máximo del período
  ]
  
  downsample-schema = "ds-otel-cumulative-histogram"
}
```

**Características:**
- `detectDrops=true` — Detecta resets de contadores
- `counter=true` — Trata el histograma como acumulativo
- Incluye `min` y `max` para estadísticas precisas

#### `otel-delta-histogram`

Para histogramas OTel con temporalidad delta (cada valor es el incremento desde la última medición).

```hocon
otel-delta-histogram {
  columns = [
    "timestamp:ts",
    "sum:double",
    "count:double",
    "h:hist:delta=true",
    "min:double",
    "max:double"
  ]
  
  value-column = "h"
  
  downsamplers = [
    "tTime(0)",
    "dSum(1)",     # Suma de sums
    "dSum(2)",     # Suma de counts
    "dSum(3)",     # Suma de histogramas
    "dMin(4)",     # Mínimo
    "dMax(5)"      # Máximo
  ]
  
  downsample-schema = "ds-otel-delta-histogram"
}
```

**Características:**
- `delta=true` — Los valores son incrementales
- Downsampling usa `dSum` (suma de deltas)

#### `otel-exp-delta-histogram`

Para histogramas OTel con buckets exponenciales y temporalidad delta.

```hocon
otel-exp-delta-histogram {
  columns = [
    "timestamp:ts",
    "sum:double",
    "count:double",
    "h:hist:exp=true:delta=true",
    "min:double",
    "max:double"
  ]
  
  value-column = "h"
  
  downsamplers = [
    "tTime(0)",
    "dSum(1)",
    "dSum(2)",
    "dSum(3)",
    "dMin(4)",
    "dMax(5)"
  ]
}
```

**Características:**
- `exp=true` — Buckets exponenciales (base 2)
- Más eficiente para distribuciones de latencia
- Compatible con el formato de histograma exponencial de OTel

### Esquemas Pre-agregados OTel

Para datos que ya vienen pre-agregados (por ejemplo, desde un collector OTel con procesador de agregación):

| Esquema | Descripción |
|---------|-------------|
| `preagg-otel-delta-histogram` | Histograma delta pre-agregado |
| `preagg-otel-exp-delta-histogram` | Histograma exponencial delta pre-agregado |

---

## 3. Formatos de Datos OTel que Puedes Almacenar

### Tabla de Compatibilidad

| Tipo de Dato OTel | Esquema FiloDB | Soportado |
|-------------------|----------------|-----------|
| **Gauge** | `gauge` | ✅ |
| **Sum (acumulativo)** | `prom-counter` | ✅ |
| **Sum (delta)** | `delta-counter` | ✅ |
| **Histogram (acumulativo)** | `otel-cumulative-histogram` | ✅ |
| **Histogram (delta)** | `otel-delta-histogram` | ✅ |
| **ExponentialHistogram (delta)** | `otel-exp-delta-histogram` | ✅ |
| **Summary** | `prom-histogram` (parcial) | ⚠️ Parcial |

### Flujo de Datos OTel → FiloDB

```
┌──────────────────────────────────────────────────────────────┐
│                  Aplicación Instrumentada                      │
│                                                               │
│  OpenTelemetry SDK                                            │
│  ├── Gauge: cpu_usage = 0.85                                  │
│  ├── Counter: http_requests = 12345                           │
│  └── Histogram: request_duration (buckets + sum + count)      │
└───────────────────────────┬──────────────────────────────────┘
                            │ OTLP (gRPC/HTTP)
                            ▼
┌──────────────────────────────────────────────────────────────┐
│              OpenTelemetry Collector                           │
│                                                               │
│  Receivers → Processors → Exporters                           │
│                              └── Prometheus Remote Write      │
└───────────────────────────┬──────────────────────────────────┘
                            │ Remote Write
                            ▼
┌──────────────────────────────────────────────────────────────┐
│              FiloDB Gateway                                    │
│                                                               │
│  Identifica el tipo de métrica:                               │
│  ├── Gauge → writeGaugeRecord()                               │
│  ├── Counter → writePromCounterRecord()                       │
│  ├── Histogram cumulative → writeOtelCumulativeHistRecord()   │
│  ├── Histogram delta → writeOtelDeltaHistRecord()             │
│  └── ExpHistogram delta → writeOtelExpDeltaHistRecord()       │
└───────────────────────────┬──────────────────────────────────┘
                            │ Kafka
                            ▼
┌──────────────────────────────────────────────────────────────┐
│              FiloDB MemStore                                   │
│                                                               │
│  Almacena con el esquema apropiado                            │
│  Compresión especializada por tipo                            │
│  Indexación Lucene por labels                                 │
└──────────────────────────────────────────────────────────────┘
```

---

## 4. Configuración del Collector OTel para FiloDB

### Ejemplo de configuración del OpenTelemetry Collector

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"
      http:
        endpoint: "0.0.0.0:4318"

processors:
  batch:
    send_batch_size: 1000
    timeout: 10s

exporters:
  prometheusremotewrite:
    endpoint: "http://filodb-gateway:9175/remote/write"
    tls:
      insecure: true
    resource_to_telemetry_conversion:
      enabled: true

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheusremotewrite]
```

### Ejemplo: Instrumentación de una Aplicación

```java
// Java - Aplicación instrumentada con OTel SDK
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.DoubleHistogram;

Meter meter = openTelemetry.getMeter("my-service");

// Contador
LongCounter requestCounter = meter.counterBuilder("http.requests")
    .setDescription("Total HTTP requests")
    .setUnit("1")
    .build();

requestCounter.add(1, Attributes.of(
    AttributeKey.stringKey("method"), "GET",
    AttributeKey.stringKey("status"), "200"
));

// Histograma
DoubleHistogram latencyHistogram = meter.histogramBuilder("http.request.duration")
    .setDescription("HTTP request duration")
    .setUnit("s")
    .build();

latencyHistogram.record(0.045, Attributes.of(
    AttributeKey.stringKey("method"), "GET",
    AttributeKey.stringKey("handler"), "/api/users"
));

// Gauge
meter.gaugeBuilder("system.memory.usage")
    .setDescription("Memory usage")
    .setUnit("By")
    .buildWithCallback(measurement -> {
        measurement.record(Runtime.getRuntime().totalMemory());
    });
```

---

## 5. Consultas sobre Datos OTel

Los datos OTel almacenados en FiloDB se consultan con PromQL estándar:

```bash
# Consultar un gauge OTel
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query' \
  --data-urlencode 'query=system_memory_usage_bytes{service_name="my-app"}' \
  --data-urlencode 'time=1609461000'

# Tasa de un counter OTel
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query_range' \
  --data-urlencode 'query=rate(http_requests_total{service_name="my-app"}[5m])' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609461000' \
  --data-urlencode 'step=60'

# Percentil de un histograma OTel
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query_range' \
  --data-urlencode 'query=histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609461000' \
  --data-urlencode 'step=60'
```

---

## 6. Ventajas de OTel en FiloDB

| Ventaja | Descripción |
|---------|-------------|
| **Histogramas nativos** | Soporte para histogramas acumulativos, delta y exponenciales |
| **Compresión optimizada** | Hasta 50x de ahorro en histogramas vs. formato raw |
| **Multi-temporalidad** | Soporta tanto acumulativo como delta |
| **Min/Max** | Campos dedicados para estadísticas precisas (no interpoladas) |
| **Pre-agregación** | Esquemas para datos ya agregados |
| **PromQL transparente** | Los datos OTel se consultan con PromQL estándar |

---

## Dependencias OTel en FiloDB

```scala
// project/Dependencies.scala
val otelVersion     = "1.54.1"
val otelInstVersion = "2.20.1-alpha"

// API y SDK
"io.opentelemetry" % "opentelemetry-api"             % otelVersion
"io.opentelemetry" % "opentelemetry-sdk-metrics"      % otelVersion
"io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % otelVersion

// Exportadores
"io.opentelemetry" % "opentelemetry-exporter-otlp"    % otelVersion
"io.opentelemetry" % "opentelemetry-exporter-logging"  % otelVersion

// Instrumentación de Runtime
"io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java8" % otelInstVersion
"io.opentelemetry.instrumentation" % "opentelemetry-oshi" % otelInstVersion
```
