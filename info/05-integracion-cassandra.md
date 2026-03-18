# 05 — Integración con Apache Cassandra

## Visión General

Apache Cassandra es la **capa de persistencia durable** de FiloDB. Mientras los datos en tiempo real se sirven desde la memoria (MemStore off-heap), Cassandra almacena los chunks comprimidos para recuperación y consultas históricas.

```
┌────────────────────────────────────────────────────────────────┐
│                     FiloDB Node                                 │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              MemStore (Off-Heap Memory)                   │  │
│  │                                                           │  │
│  │  Datos recientes (últimas horas)                          │  │
│  │  Índice Lucene de particiones                             │  │
│  │  Buffers de ingesta                                       │  │
│  └───────────────────────┬──────────────────────────────────┘  │
│                          │ Flush periódico                      │
│                          │ (cada 1 hora por defecto)            │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Cassandra Sink                               │  │
│  │                                                           │  │
│  │  CassandraChunkSink → Escribe chunks comprimidos          │  │
│  │  CassandraChunkSource → Lee chunks para recuperación      │  │
│  │  CassandraMetaStore → Metadatos y checkpoints             │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────┐
│                    Apache Cassandra                              │
│                                                                 │
│  Keyspace: filodb (datos raw)                                   │
│  Keyspace: filodb_downsample (datos downsampled)                │
│  Keyspace: filodb_admin (checkpoints y metadatos)               │
└────────────────────────────────────────────────────────────────┘
```

---

## Esquema de Base de Datos

FiloDB crea tres keyspaces principales en Cassandra:

### 1. Keyspace de Administración (`filodb_admin`)

#### Tabla: `checkpoints`
Almacena los offsets de Kafka para recuperación.

```cql
CREATE TABLE IF NOT EXISTS filodb_admin.checkpoints (
    databasename text,
    datasetname text,
    shardnum int,
    groupnum int,
    offset bigint,
    PRIMARY KEY ((databasename, datasetname, shardnum, groupnum))
);
```

### 2. Keyspace de Datos Raw (`filodb`)

#### Tabla: `{dataset}_tschunks`
Almacena los chunks de series temporales comprimidos.

```cql
CREATE TABLE IF NOT EXISTS filodb.prometheus_tschunks (
    partition blob,
    chunkid bigint,
    info blob,
    chunks frozen<list<blob>>,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};
```

**Campos:**
- `partition` — Clave de partición binaria (hash de labels)
- `chunkid` — ID del chunk (basado en timestamp)
- `info` — Metadatos del chunk (rango de tiempo, cuenta de registros)
- `chunks` — Lista de vectores comprimidos (uno por columna)

#### Tabla: `{dataset}_ingestion_time_index`
Índice por tiempo de ingestión para consultas eficientes.

```cql
CREATE TABLE IF NOT EXISTS filodb.prometheus_ingestion_time_index (
    partition blob,
    ingestion_time bigint,
    start_time bigint,
    info blob,
    PRIMARY KEY (partition, ingestion_time, start_time)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'};
```

#### Tablas: `{dataset}_partitionkeys_{shard}`
Claves de partición por shard (una tabla por shard).

```cql
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeys_0 (
    partkey blob,
    starttime bigint,
    endtime bigint,
    PRIMARY KEY (partkey)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'};
```

#### Tabla: `{dataset}_partitionkeysv2`
Versión 2 del índice de particiones, más eficiente.

```cql
CREATE TABLE IF NOT EXISTS filodb.prometheus_partitionkeysv2 (
    shard int,
    bucket int,
    partkey blob,
    starttime bigint,
    endtime bigint,
    PRIMARY KEY ((shard, bucket), partkey)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'};
```

#### Tabla: `{dataset}_pks_by_update_time`
Particiones indexadas por tiempo de actualización.

```cql
CREATE TABLE IF NOT EXISTS filodb.prometheus_pks_by_update_time (
    shard int,
    epochhour bigint,
    split int,
    partkey blob,
    starttime bigint,
    endtime bigint,
    PRIMARY KEY ((shard, epochhour, split), partkey)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'};
```

### 3. Keyspace de Downsampling (`filodb_downsample`)

#### Tablas: `{dataset}_ds_{resolution}_tschunks`
Chunks downsampled a diferentes resoluciones.

```cql
-- Resolución 1 minuto
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_1_tschunks (
    partition blob,
    chunkid bigint,
    info blob,
    chunks frozen<list<blob>>,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
  AND default_time_to_live = 2592000;  -- 30 días

-- Resolución 5 minutos
CREATE TABLE IF NOT EXISTS filodb_downsample.prometheus_ds_5_tschunks (
    partition blob,
    chunkid bigint,
    info blob,
    chunks frozen<list<blob>>,
    PRIMARY KEY (partition, chunkid)
) WITH compression = {'chunk_length_in_kb': '16', 'class': 'LZ4Compressor'}
  AND default_time_to_live = 15811200;  -- 183 días
```

---

## Script de Creación de Esquema

FiloDB incluye un script para generar el DDL de Cassandra:

**Ubicación:** `scripts/schema-create.sh`

```bash
# Uso:
# ./scripts/schema-create.sh <admin_keyspace> <raw_keyspace> <downsample_keyspace> <dataset> <num_shards> <resolutions>

# Ejemplo: Crear esquema para 4 shards con resoluciones 1min y 5min
./scripts/schema-create.sh filodb_admin filodb filodb_downsample prometheus 4 1,5 > /tmp/ddl.cql

# Ejecutar el DDL en Cassandra
cqlsh -f /tmp/ddl.cql
```

**El script genera:**
1. Creación de keyspaces con replicación configurable
2. Tablas de chunks para datos raw
3. Tablas de índices
4. Tablas de particiones por shard
5. Tablas de downsampling por resolución
6. Tabla de checkpoints

---

## Configuración de Cassandra en FiloDB

### Configuración del Servidor

**Archivo:** `conf/timeseries-filodb-server.conf`

```hocon
filodb {
  cassandra {
    # Hosts de Cassandra
    hosts = "localhost"
    port = 9042
    
    # Keyspaces
    admin-keyspace = "filodb_admin"
    keyspace = "filodb"
    downsample-keyspace = "filodb_downsample"
    
    # Nivel de consistencia
    read-consistency = "ONE"
    write-consistency = "ONE"
    
    # Pool de conexiones
    connections-per-host-core = 2
    connections-per-host-max = 4
    
    # Timeouts
    connect-timeout = 5000    # ms
    read-timeout = 12000      # ms
    
    # Compresión
    lz4-chunk-compress = true
    
    # Reintento
    retry-policy = "DefaultRetryPolicy"
  }
}
```

### Configuración del Store

```hocon
store {
  # Intervalo de flush de MemStore a Cassandra
  flush-interval = 1h
  
  # TTL de los datos en Cassandra
  disk-time-to-live = 24 hours
  
  # Tamaño de memoria por shard
  shard-mem-size = 256MB
  
  # Memoria para buffers de ingesta
  ingestion-buffer-mem-size = 200MB
}
```

---

## Flujo de Datos: Memoria → Cassandra

### Proceso de Flush

```
Tiempo ──────────────────────────────────────────────────────────►
│                                                                 │
│  Ingesta continua          Flush                   Flush        │
│  ──────────────►          ─────►                  ─────►        │
│                                                                 │
│  ┌─── Grupo 0 ───┐  ┌─── Grupo 1 ───┐  ┌─── Grupo 0 ───┐     │
│  │ Chunks en mem  │  │ Chunks en mem  │  │ Chunks en mem  │     │
│  │ sin persistir  │  │ sin persistir  │  │ sin persistir  │     │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘     │
│          │                   │                   │               │
│          ▼                   ▼                   ▼               │
│    ┌──────────┐        ┌──────────┐        ┌──────────┐         │
│    │Cassandra │        │Cassandra │        │Cassandra │         │
│    │  Write   │        │  Write   │        │  Write   │         │
│    └──────────┘        └──────────┘        └──────────┘         │
```

**Detalles del flush:**
1. Los chunks se comprimen usando Delta-delta (timestamps), XOR (doubles), NibblePacking
2. Se escriben como blobs en la tabla `_tschunks`
3. Se actualiza el índice de tiempo de ingestión
4. Se guarda el checkpoint del offset de Kafka
5. Los chunks flushed se pueden desalojar de la memoria (demand paging)

---

## Recuperación desde Cassandra

Cuando un nodo se reinicia, el proceso de recuperación es:

1. **Carga de índice de particiones**
   - Lee `_partitionkeysv2` para el shard asignado
   - Reconstruye el índice Lucene en memoria

2. **Recuperación de chunks recientes**
   - Lee los chunks más recientes de `_tschunks`
   - Los carga en el MemStore (off-heap)

3. **Replay de Kafka**
   - Lee el último checkpoint de `checkpoints`
   - Re-consume datos de Kafka desde ese offset

```
Reinicio del nodo:
1. Lee partitionkeys  ──► Reconstruye índice Lucene
2. Lee tschunks       ──► Carga chunks en MemStore
3. Lee checkpoints    ──► Resume consumo de Kafka
4. Consume Kafka      ──► Datos nuevos al MemStore
```

---

## Compresión en Cassandra

FiloDB aplica **doble compresión**:

1. **Compresión a nivel de FiloDB** (antes de escribir):
   - Timestamps: Delta-delta encoding
   - Doubles: XOR encoding (similar a Gorilla)
   - Longs: Delta encoding
   - Histogramas: Compresión incremental (hasta 50x)
   - NibblePacking para valores pequeños

2. **Compresión a nivel de Cassandra** (en disco):
   - LZ4 con chunks de 16KB

**Resultado:** Los datos ocupan muy poco espacio en disco comparado con formatos raw.

---

## Estrategia de Compactación

FiloDB usa `TimeWindowCompactionStrategy` en Cassandra, que es ideal para series temporales porque:

- Agrupa SSTable por ventanas de tiempo
- Los datos viejos se compactan juntos
- Los TTL eliminan datos expirados eficientemente
- Reduce la amplificación de escritura

---

## Requisitos de Cassandra

| Aspecto | Recomendación |
|---------|---------------|
| **Versión** | Cassandra 2.x o 3.x |
| **Replicación** | SimpleStrategy (dev) o NetworkTopologyStrategy (prod) |
| **Factor de replicación** | 1 (dev), 3 (prod) |
| **Consistencia** | ONE (dev), LOCAL_QUORUM (prod) |
| **Disco** | SSD recomendado |
| **Memoria** | Suficiente para caches de Cassandra |
| **JVM** | Java 8 o 11 |
