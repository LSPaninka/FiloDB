# 03 — Frameworks y Dependencias

FiloDB utiliza un ecosistema rico de frameworks y librerías del mundo JVM (Scala/Java). A continuación se detalla cada dependencia principal, su versión y su rol dentro del sistema.

---

## Sistema de Build

| Herramienta | Versión | Propósito |
|-------------|---------|-----------|
| **SBT** | 1.x | Build tool principal (Scala Build Tool) |
| **Scala** | 2.12.12 | Lenguaje de programación principal |
| **Java** | 11+ | Entorno de ejecución (JVM) |

**Archivos de configuración del build:**
- `build.sbt` — Agregación raíz de subproyectos
- `project/FiloBuild.scala` — Definición de módulos y dependencias
- `project/Dependencies.scala` — Catálogo centralizado de dependencias
- `project/FiloSettings.scala` — Configuraciones de compilación
- `version.sbt` — Versión del proyecto (0.9-SNAPSHOT)

---

## Frameworks Principales

### 1. Akka (Cluster, HTTP, Streams)

| Componente | Versión | Uso |
|------------|---------|-----|
| `akka-actor` | 2.5.22 | Sistema de actores para concurrencia |
| `akka-cluster` | 2.5.22 | Clustering y distribución de shards |
| `akka-remote` | 2.5.22 | Comunicación entre nodos |
| `akka-http` | 10.1.8 | Servidor REST/HTTP |
| `akka-stream` | 2.5.22 | Procesamiento de flujos reactivos |
| `akka-slf4j` | 2.5.22 | Logging integration |

**Rol:** Akka es el framework central que permite a FiloDB funcionar como un sistema distribuido. Cada nodo de FiloDB es un cluster Akka con actores para ingesta, consulta, y coordinación de shards.

### 2. Apache Cassandra (Driver)

| Componente | Versión | Uso |
|------------|---------|-----|
| `cassandra-driver-core` | 3.7.1 | Cliente CQL para persistencia |

**Rol:** Cassandra es la capa de persistencia durable. Los chunks comprimidos y los índices de partición se almacenan en tablas de Cassandra.

### 3. Apache Kafka (Client)

| Componente | Versión | Uso |
|------------|---------|-----|
| `kafka-clients` | 3.6.2 | Consumo de datos de Kafka |
| `monix-kafka` | 1.0.0-RC6 | Integración reactiva Kafka-Monix |

**Rol:** Kafka es el sistema de ingestión primario. Los datos llegan a topics de Kafka en formato RecordContainer y son consumidos por los nodos de FiloDB.

### 4. Apache Spark

| Componente | Versión | Uso |
|------------|---------|-----|
| `spark-core` | 3.4.0 | Motor de procesamiento batch |
| `spark-sql` | 3.4.0 | Procesamiento de datos estructurados |
| `spark-streaming` | 3.4.0 | Streaming (legacy) |

**Rol:** Spark se utiliza para jobs de downsampling que se ejecutan periódicamente (cada 6 horas), reduciendo la resolución de datos históricos.

### 5. Apache Lucene

| Componente | Versión | Uso |
|------------|---------|-----|
| `lucene-core` | 9.7.0 | Motor de indexación |
| `lucene-queries` | 9.7.0 | Consultas sobre índices |

**Rol:** Lucene provee indexación eficiente por etiquetas (labels) de series temporales, permitiendo búsquedas rápidas por combinaciones de tags.

### 6. OpenTelemetry

| Componente | Versión | Uso |
|------------|---------|-----|
| `opentelemetry-api` | 1.54.1 | API de instrumentación |
| `opentelemetry-sdk-metrics` | 1.54.1 | SDK de métricas |
| `opentelemetry-exporter-otlp` | 1.54.1 | Exportador OTLP |
| `opentelemetry-exporter-logging` | 1.54.1 | Exportador a logs |
| `opentelemetry-runtime-telemetry-java8` | 2.20.1-alpha | Métricas de runtime JVM |
| `opentelemetry-oshi` | 2.20.1-alpha | Métricas de sistema |

**Rol:** OpenTelemetry se usa para instrumentar FiloDB internamente (métricas de rendimiento, JVM, sistema) y también define esquemas para almacenar datos OTel.

### 7. gRPC y Protocol Buffers

| Componente | Versión | Uso |
|------------|---------|-----|
| `grpc-netty` | 1.50.0 | Transporte gRPC |
| `grpc-services` | 1.50.0 | Servicios gRPC base |
| `protobuf-java` | 3.21.7 | Serialización Protocol Buffers |
| `scalapb-runtime` | 0.11.13 | Generación de código Scala para protobuf |

**Rol:** gRPC permite la comunicación eficiente entre nodos del cluster para consultas distribuidas y también se usa para el servidor de consultas PromQL.

---

## Librerías de Soporte

### Serialización y Compresión

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `kryo` | 4.0.0 | Serialización binaria de mensajes Akka |
| `akka-kryo-serialization` | 1.0.0 | Integración Kryo con Akka |
| `lz4-java` | 1.4 | Compresión LZ4 de chunks |
| `snappy-java` | — | Compresión Snappy (remote read) |

### Almacenamiento Local

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `rocksdbjni` | 6.29.5 | Base de datos embebida para estado local |

### Métricas y Monitoreo

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `kamon-bundle` | 2.7.3 | Instrumentación de métricas (alternativa/complemento a OTel) |
| `kamon-prometheus` | 2.7.3 | Exportación de métricas en formato Prometheus |

### Logging

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `logback-classic` | 1.5.6 | Framework de logging |
| `scala-logging` | 3.7.2 | Wrapper Scala para SLF4J |
| `log4j-to-slf4j` | 2.13.3 | Bridge Log4j → SLF4J |

### JSON

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `circe-core` | 0.9.3 | Codificación/decodificación JSON |
| `circe-generic` | 0.9.3 | Derivación automática de codecs |
| `circe-parser` | 0.9.3 | Parsing de JSON |
| `akka-http-circe` | 1.21.0 | Integración Circe con Akka HTTP |

### Estadísticas y Algoritmos

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `t-digest` | 3.1 | Estimación de percentiles |
| `datasketches-java` | 3.0.0 | Algoritmos probabilísticos (sketches) |

### CLI y Utilidades

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `scallop` | 3.1.1 | Parser de argumentos de línea de comandos |
| `opencsv` | 3.3 | Lectura/escritura de archivos CSV |
| `typesafe-config` | 1.3.2 | Configuración HOCON/JSON |

### Memoria Nativa

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `jnr-ffi` | 2.1.7 | Foreign Function Interface para memoria off-heap |
| `jnr-posix` | — | Acceso a APIs POSIX |

### Testing

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `scalatest` | 3.0.8 | Framework de testing |
| `akka-testkit` | 2.5.22 | Testing de actores Akka |
| `akka-http-testkit` | 10.1.8 | Testing de rutas HTTP |
| `scalacheck` | 1.13.5 | Testing basado en propiedades |

---

## Diagrama de Stack Tecnológico

```
┌─────────────────────────────────────────────────────────────────┐
│                        Aplicación FiloDB                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Lenguaje: Scala 2.12 / Java 11                                │
│  Build: SBT                                                     │
│                                                                 │
├────────────┬────────────┬────────────┬─────────────────────────┤
│   Akka     │  Akka HTTP │   gRPC     │    Netty (Gateway)      │
│  Cluster   │  REST API  │  Inter-nodo│    Ingesta TCP          │
│  2.5.22    │  10.1.8    │  1.50.0    │                         │
├────────────┴────────────┴────────────┴─────────────────────────┤
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Lucene   │  │ RocksDB  │  │ LZ4/XOR  │  │ Kryo     │       │
│  │ Indexing  │  │ Local    │  │ Compress │  │ Serial.  │       │
│  │ 9.7.0    │  │ 6.29.5   │  │          │  │ 4.0.0    │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│                                                                 │
├────────────┬────────────┬────────────┬─────────────────────────┤
│  Cassandra │  Kafka     │  Spark     │   OpenTelemetry         │
│  Driver    │  Client    │  3.4.0     │   SDK 1.54.1            │
│  3.7.1     │  3.6.2     │            │                         │
├────────────┴────────────┴────────────┴─────────────────────────┤
│                                                                 │
│  Monitoreo: Kamon 2.7.3 + OpenTelemetry 1.54.1                │
│  JSON: Circe 0.9.3                                             │
│  Config: Typesafe Config 1.3.2                                 │
│  Logging: Logback 1.5.6 + Scala-Logging 3.7.2                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Plugins de SBT

| Plugin | Propósito |
|--------|-----------|
| `sbt-assembly` | Creación de fat JARs |
| `sbt-multi-jvm` | Testing multi-JVM |
| `sbt-jmh` | Benchmarks JMH |
| `sbt-scalapb` | Generación de código protobuf |
| `gatling-sbt` | Tests de carga Gatling |
