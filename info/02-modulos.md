# 02 — Módulos de la Aplicación

FiloDB está compuesto por **14 módulos principales** organizados como subproyectos SBT. Cada módulo tiene una responsabilidad específica y se comunica con otros a través de interfaces bien definidas.

---

## Diagrama de Dependencias entre Módulos

```
                                    ┌──────────────┐
                                    │  standalone   │
                                    │ (Server App)  │
                                    └───────┬───────┘
                    ┌───────┬───────┬───────┼───────┬───────┬──────────┐
                    ▼       ▼       ▼       ▼       ▼       ▼          ▼
              ┌─────────┐ ┌─────┐ ┌──────┐ ┌────┐ ┌───────┐ ┌─────────────┐
              │  kafka   │ │http │ │gateway│ │grpc│ │cassan.│ │bootstrapper │
              └────┬─────┘ └──┬──┘ └──┬───┘ └──┬─┘ └───┬───┘ └─────────────┘
                   │          │       │        │       │
                   ▼          ▼       ▼        ▼       ▼
              ┌──────────────────────────────────────────────┐
              │              coordinator                      │
              │    (Cluster coordination, ingestion mgmt)     │
              └──────────────────┬───────────────────────────┘
                   ┌─────────────┼─────────────┐
                   ▼             ▼             ▼
            ┌────────────┐ ┌─────────┐ ┌────────────┐
            │ prometheus  │ │  query  │ │    cli     │
            │ (PromQL)    │ │ (Engine)│ │ (Terminal) │
            └──────┬──────┘ └────┬────┘ └────────────┘
                   │             │
                   ▼             ▼
              ┌──────────────────────────────────────────┐
              │                  core                     │
              │    (Engine, MemStore, schemas, indices)   │
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
              ┌──────────────────────────────────────────┐
              │                 memory                    │
              │    (Off-heap allocation, encoding)        │
              └──────────────────────────────────────────┘
```

---

## Descripción Detallada de Cada Módulo

### 1. `memory` — Gestión de Memoria Off-Heap

**Ruta:** `memory/`

El módulo más bajo en la jerarquía. Gestiona la asignación de memoria fuera del heap de JVM para evitar pausas de garbage collection.

**Responsabilidades:**
- Asignación y liberación de bloques de memoria off-heap
- Codificación y decodificación de vectores binarios (BinaryVector)
- Compresión de datos: Delta-delta, XOR, NibblePacking, LZ4
- Formato de registros binarios (BinaryRecord)
- Gestión de buffers de escritura y lectura

**Clases clave:**
- `MemFactory` — Fábrica de bloques de memoria
- `BinaryVector` — Vectores binarios comprimidos
- `NativeMemoryManager` — Gestión de memoria nativa vía JNR-FFI

**Dependencias externas:** JNR-FFI, LZ4-Java, Joda-Time

---

### 2. `core` — Motor Principal

**Ruta:** `core/`

El corazón de FiloDB. Contiene el motor de almacenamiento en memoria, los esquemas de datos, el índice de particiones, y la lógica de flush a disco.

**Responsabilidades:**
- `TimeSeriesMemStore` — Almacén de series temporales en memoria
- Definición de esquemas (gauge, counter, histogram, delta, OTel)
- Índice de particiones basado en Apache Lucene
- Gestión de chunks (bloques de datos comprimidos)
- Serialización con Kryo
- Métricas con OpenTelemetry y Kamon
- Configuración global (`filodb-defaults.conf`)

**Clases clave:**
- `TimeSeriesMemStore` — Store principal en memoria
- `PartitionIndex` — Índice Lucene para búsqueda por tags
- `ChunkSet` — Conjunto de chunks comprimidos
- `DataSchema` / `PartitionSchema` — Definición de esquemas
- `FilodbMetrics` — Instrumentación OTel

**Dependencias externas:** Apache Lucene 9.7, RocksDB, Kryo, OpenTelemetry SDK

---

### 3. `query` — Motor de Consultas

**Ruta:** `query/`

Motor de ejecución de consultas que transforma planes lógicos en planes de ejecución distribuidos.

**Responsabilidades:**
- Transformación de LogicalPlan → ExecPlan
- Ejecución de consultas distribuidas entre shards
- Selección basada en costos (datos raw vs. downsampled)
- Evaluación de funciones PromQL (rate, sum, avg, etc.)
- Producción de RangeVectors (resultados)
- Comunicación gRPC entre nodos

**Pipeline de consulta:**
```
PromQL String → AST → LogicalPlan → ExecPlan → RangeVectors
```

**Clases clave:**
- `LogicalPlan` — Plan lógico abstracto
- `ExecPlan` — Plan de ejecución concreto
- `RangeVector` — Vector de resultados
- `QueryPlanner` — Planificador de consultas

**Dependencias externas:** T-Digest, DataSketches, gRPC

---

### 4. `prometheus` — Soporte PromQL

**Ruta:** `prometheus/`

Parsing y conversión del lenguaje de consultas PromQL de Prometheus.

**Responsabilidades:**
- Parsing de expresiones PromQL
- Conversión de AST de PromQL a LogicalPlan de FiloDB
- Soporte de funciones agregadas, selectores, matchers
- Validación de consultas

**Clases clave:**
- `Parser` — Parser de PromQL
- `PromQLtoLogicalPlan` — Convertidor de PromQL a plan lógico

---

### 5. `coordinator` — Coordinación del Cluster

**Ruta:** `coordinator/`

Gestiona la coordinación del cluster Akka, la asignación de shards, y la orquestación de ingestión y consultas.

**Responsabilidades:**
- Asignación de shards a nodos del cluster
- Coordinación de la ingesta de datos
- Enrutamiento de consultas a los shards correctos
- Gestión del ciclo de vida del cluster (join, leave, failure)
- Checkpointing de offsets de Kafka
- Recuperación tras fallos

**Clases clave:**
- `NodeClusterActor` — Actor principal del cluster
- `ShardManager` — Gestor de shards
- `IngestionActor` — Actor de ingesta por shard
- `QueryActor` — Actor de consultas
- `ShardMapper` — Mapeo de particiones a shards

**Dependencias externas:** Akka Cluster, Akka Kryo Serialization

---

### 6. `cassandra` — Capa de Persistencia

**Ruta:** `cassandra/`

Implementación del almacenamiento persistente usando Apache Cassandra.

**Responsabilidades:**
- Escritura de chunks comprimidos a Cassandra
- Lectura de chunks para recuperación
- Almacenamiento de claves de partición
- Gestión de checkpoints de ingestión
- Esquemas de tablas (tschunks, partitionkeys, etc.)

**Clases clave:**
- `CassandraChunkSink` — Escritor de chunks
- `CassandraChunkSource` — Lector de chunks
- `CassandraMetaStore` — Metadatos en Cassandra

**Dependencias externas:** DataStax Cassandra Driver 3.7.1

---

### 7. `kafka` — Integración con Kafka

**Ruta:** `kafka/`

Factoría de streams de ingestión desde Apache Kafka.

**Responsabilidades:**
- Creación de streams de consumo de Kafka
- Deserialización de RecordContainers
- Gestión de offsets y grupos de consumidores
- Manejo de fallos de consumo

**Clases clave:**
- `KafkaIngestionStreamFactory` — Fábrica de streams
- `RecordContainerDeserializer` — Deserializador de registros binarios

**Dependencias externas:** Kafka Clients 3.6.2, Monix-Kafka

---

### 8. `http` — Servidor HTTP/REST

**Ruta:** `http/`

Servidor Akka HTTP que expone endpoints REST compatibles con Prometheus.

**Responsabilidades:**
- Endpoints de consulta PromQL (`/promql/{dataset}/api/v1/query_range`, etc.)
- Endpoints de administración (`/api/v1/cluster/`, `/admin/health`)
- Endpoints de metadatos (`/labels`, `/label/{name}/values`)
- Remote read de Prometheus (protobuf + Snappy)
- Descubrimiento de nodos del cluster (`/__members`)

**Clases clave:**
- `PrometheusApiRoute` — Rutas de API Prometheus
- `ClusterApiRoute` — Rutas de gestión del cluster
- `AdminRoutes` — Rutas de administración
- `HealthRoute` — Ruta de health check

**Dependencias externas:** Akka HTTP 10.1.8, Circe JSON, Akka HTTP Circe

---

### 9. `gateway` — Gateway de Ingestión

**Ruta:** `gateway/`

Servidor Netty de alto rendimiento que acepta datos en múltiples formatos y los publica en Kafka.

**Responsabilidades:**
- Aceptar Prometheus Remote Write (protobuf)
- Aceptar formato InfluxDB Line Protocol
- Conversión a RecordContainer binario
- Particionamiento por shard y publicación a Kafka
- Generación de datos de prueba

**Clases clave:**
- `GatewayServer` — Servidor principal Netty
- `InfluxProtocolParser` — Parser de protocolo InfluxDB
- `InputRecord` — Interfaz abstracta de registros de entrada

**Dependencias externas:** Netty, Protocol Buffers, ScalaPB

---

### 10. `grpc` — Definiciones gRPC

**Ruta:** `grpc/`

Definiciones de servicios y mensajes gRPC para comunicación entre nodos.

**Responsabilidades:**
- Definición de servicios gRPC (protobuf)
- Generación de stubs cliente/servidor
- Comunicación inter-nodo para consultas distribuidas

**Dependencias externas:** gRPC 1.50.0, Protocol Buffers 3.21.7

---

### 11. `cli` — Herramienta de Línea de Comandos

**Ruta:** `cli/`

Interfaz de línea de comandos para administración y consultas interactivas.

**Responsabilidades:**
- Gestión de datasets (crear, listar, eliminar)
- Ejecución de consultas PromQL desde terminal
- Inspección de metadatos y esquemas
- Depuración de datos (decode chunks, vectors)

**Clases clave:**
- `CliMain` — Punto de entrada del CLI

**Dependencias externas:** Scallop (CLI parser), OpenCSV

---

### 12. `standalone` — Servidor Completo

**Ruta:** `standalone/`

Aplicación standalone que combina todos los módulos en un servidor ejecutable.

**Responsabilidades:**
- Inicialización completa del servidor FiloDB
- Bootstrap del cluster Akka
- Inicio del servidor HTTP
- Inicio opcional del servidor gRPC
- Gestión del ciclo de vida

**Clases clave:**
- `FiloServer` — Punto de entrada principal del servidor

---

### 13. `akka-bootstrapper` — Descubrimiento de Cluster

**Ruta:** `akka-bootstrapper/`

Mecanismos de descubrimiento de seeds para el cluster Akka.

**Responsabilidades:**
- Descubrimiento por HTTP (`/__members` endpoint)
- Descubrimiento por DNS SRV
- Descubrimiento por Consul
- Soporte para Kubernetes StatefulSets

**Clases clave:**
- `AkkaBootstrapper` — Bootstrap principal
- `HttpSeedNodeDiscovery` — Descubrimiento por HTTP
- `DnsSrvSeedNodeDiscovery` — Descubrimiento por DNS
- `ConsulSeedNodeDiscovery` — Descubrimiento por Consul

---

### 14. `spark-jobs` — Jobs de Spark

**Ruta:** `spark-jobs/`

Jobs batch que se ejecutan con Apache Spark para procesamiento offline.

**Responsabilidades:**
- Downsampling de series temporales (resoluciones 1min, 5min)
- Detección de churn de labels
- Lectura paralela de Cassandra via ScanSplits
- Escritura de datos downsampled a Cassandra

**Clases clave:**
- `DownsamplerMain` — Punto de entrada del job de downsampling
- `DSPartitionReader` — Lector de particiones paralelo

**Dependencias externas:** Apache Spark 3.4.0

---

## Tabla Resumen

| Módulo | Capa | Tipo | Puerto/Protocolo |
|--------|------|------|------------------|
| memory | Base | Librería | N/A |
| core | Motor | Librería | N/A |
| query | Consulta | Librería | N/A |
| prometheus | Consulta | Librería | N/A |
| coordinator | Cluster | Librería | Akka Remoting (2552) |
| cassandra | Persistencia | Librería | CQL (9042) |
| kafka | Ingesta | Librería | Kafka (9092) |
| http | API | Servidor | HTTP (8080) |
| gateway | Ingesta | Servidor | TCP (Netty) |
| grpc | Comunicación | Servidor | gRPC (8888) |
| cli | Herramienta | Ejecutable | N/A |
| standalone | Aplicación | Servidor | HTTP + Akka + gRPC |
| akka-bootstrapper | Cluster | Librería | HTTP/DNS/Consul |
| spark-jobs | Batch | Job | Spark (local/cluster) |

---

## Módulos Auxiliares

| Módulo | Propósito |
|--------|-----------|
| `jmh/` | Benchmarks de rendimiento con JMH (Java Microbenchmark Harness) |
| `gatling/` | Tests de carga y estrés con Gatling |
| `project/` | Configuración del build SBT (Dependencies.scala, FiloBuild.scala, FiloSettings.scala) |
| `conf/` | Archivos de configuración de ejemplo para desarrollo y producción |
| `scripts/` | Scripts de utilidad (creación de esquemas Cassandra, etc.) |
