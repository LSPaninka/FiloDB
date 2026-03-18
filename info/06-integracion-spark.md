# 06 — Integración con Apache Spark

## Visión General

Apache Spark se integra con FiloDB para realizar **procesamiento batch**, principalmente para el **downsampling** de series temporales. Los jobs de Spark leen datos de alta resolución desde Cassandra y escriben datos downsampled a resoluciones menores.

```
┌─────────────────────────────────────────────────────────────────┐
│                      Spark Cluster                               │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              DownsamplerMain                              │   │
│  │                                                           │   │
│  │  1. Lee chunks de alta resolución                         │   │
│  │  2. Aplica algoritmos de downsampling                     │   │
│  │  3. Escribe chunks de baja resolución                     │   │
│  └──────────────────────────┬────────────────────────────────┘   │
│                             │                                    │
└─────────────────────────────┼────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼                               ▼
┌──────────────────────┐        ┌──────────────────────┐
│  Cassandra (Raw)     │        │  Cassandra (Downsamp)│
│                      │        │                      │
│  filodb.prometheus_  │        │  filodb_downsample.  │
│  tschunks            │        │  prometheus_ds_1_    │
│                      │        │  tschunks            │
│  Resolución: raw     │        │                      │
│  (cada 15-30s)       │        │  Resolución: 1min    │
│                      │        │  TTL: 30 días        │
│                      │        │                      │
│                      │        │  prometheus_ds_5_    │
│                      │        │  tschunks            │
│                      │        │                      │
│                      │        │  Resolución: 5min    │
│                      │        │  TTL: 183 días       │
└──────────────────────┘        └──────────────────────┘
```

---

## Módulo `spark-jobs/`

### DownsamplerMain

**Ubicación:** `spark-jobs/src/main/scala/filodb/downsampler/DownsamplerMain.scala`

El `DownsamplerMain` es el punto de entrada para el job de downsampling:

```scala
// Pseudocódigo del flujo
object DownsamplerMain {
  def main(args: Array[String]): Unit = {
    // 1. Crear SparkSession
    val spark = SparkSessionFactory.create()
    
    // 2. Leer configuración de downsampling
    val config = DownsamplerConfig.load()
    
    // 3. Para cada resolución (1min, 5min)
    for (resolution <- config.resolutions) {
      // 4. Leer chunks raw de Cassandra (paralelo por shard)
      val rawChunks = readFromCassandra(config.rawKeyspace, config.dataset)
      
      // 5. Aplicar downsampling
      val downsampled = downsample(rawChunks, resolution)
      
      // 6. Escribir a Cassandra downsampled
      writeToCassandra(config.downsampleKeyspace, downsampled, resolution)
    }
  }
}
```

### Componentes Clave

#### `DSPartitionReader`
Lee particiones de Cassandra en paralelo usando ScanSplits para distribución eficiente entre executors de Spark.

#### `ChunkPersistor`
Interfaz pluggable para escritura de chunks downsampled. Permite implementaciones custom para diferentes backends.

#### `SparkSessionFactory`
Fábrica pluggable para crear SparkSessions. Útil para configurar Spark en diferentes entornos (local, YARN, Kubernetes).

---

## Algoritmos de Downsampling

FiloDB soporta varios algoritmos de downsampling según el tipo de dato:

### Para Gauges (valores instantáneos)

| Algoritmo | Descripción | Uso |
|-----------|-------------|-----|
| **dMin** | Mínimo del período | Valor más bajo en la ventana |
| **dMax** | Máximo del período | Valor más alto en la ventana |
| **dSum** | Suma del período | Total acumulado en la ventana |
| **dAvg** | Promedio del período | Media aritmética |
| **dLast** | Último valor | Valor más reciente |
| **tTime** | Timestamp del período | Marca temporal representativa |

### Para Counters (valores acumulativos)

| Algoritmo | Descripción | Uso |
|-----------|-------------|-----|
| **dLast** | Último valor del counter | Preserva monotonicidad |
| **tTime** | Timestamp | Referencia temporal |

Los counters detectan resets (cuando el valor disminuye) y los manejan correctamente en el downsampling.

### Para Histogramas

| Algoritmo | Descripción |
|-----------|-------------|
| **dLast** | Último histograma del período |
| **dSum** | Suma de buckets del período |
| **dMin** | Mínimo por bucket |
| **dMax** | Máximo por bucket |

---

## Configuración de Downsampling

### En la Fuente de Datos

```hocon
# conf/timeseries-dev-source.conf
sourceconfig {
  downsample {
    # Resoluciones en minutos
    resolutions = [1, 5]
    
    # TTLs en días (correspondientes a cada resolución)
    ttls = [30, 183]
    
    # Cambios de esquema por resolución
    raw-schema-changes = []
  }
}
```

### Esquemas de Downsampling

Cada esquema define sus propios downsamplers en `filodb-defaults.conf`:

```hocon
# Esquema gauge
schemas {
  gauge {
    columns = ["timestamp:ts", "value:double"]
    downsamplers = ["tTime(0)", "dMin(1)", "dMax(1)", "dSum(1)", "dCount(1)", "dAvg(1)"]
    downsample-schema = "ds-gauge"
  }
  
  # Esquema downsampled de gauge
  ds-gauge {
    columns = [
      "timestamp:ts",
      "min:double",
      "max:double", 
      "sum:double",
      "count:double",
      "avg:double"
    ]
  }
  
  # Esquema counter
  prom-counter {
    columns = ["timestamp:ts", "value:double:detectDrops=true"]
    downsamplers = ["tTime(0)", "dLast(1)"]
    downsample-schema = "ds-prom-counter"
  }
}
```

---

## Ciclo de Ejecución del Job

Los jobs de downsampling se ejecutan periódicamente, típicamente **cada 6 horas**:

```
Día típico de ejecución:
├── 02:00 UTC — Job de downsampling (procesa 20:00-02:00)
├── 08:00 UTC — Job de downsampling (procesa 02:00-08:00)
├── 14:00 UTC — Job de downsampling (procesa 08:00-14:00)
└── 20:00 UTC — Job de downsampling (procesa 14:00-20:00)
```

### Ventana de Procesamiento

```
Ventana del job (ejemplo: ejecución a las 08:00 UTC):

Tiempo de ingestión consultado: 22:00 (día anterior) → 08:00 (actual)
                                 ▲ Incluye buffer de 4h

Tiempo de usuario (datos): 02:00 → 08:00 (alineado a ventana de 6h)

Los chunks se alinean a ventanas de 6 horas para permitir
reparaciones cross-DC (data center).
```

---

## Ejecución del Job de Spark

### Local (desarrollo)

```bash
# Compilar el jar
sbt spark-jobs/assembly

# Ejecutar localmente
spark-submit \
  --class filodb.downsampler.DownsamplerMain \
  --master local[4] \
  --conf spark.filodb.cassandra.hosts=localhost \
  --conf spark.filodb.cassandra.port=9042 \
  spark-jobs/target/scala-2.12/spark-jobs-assembly-0.9-SNAPSHOT.jar
```

### Cluster (producción)

```bash
spark-submit \
  --class filodb.downsampler.DownsamplerMain \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-memory 4g \
  --executor-cores 2 \
  --conf spark.filodb.cassandra.hosts=cass1,cass2,cass3 \
  --conf spark.filodb.cassandra.port=9042 \
  --conf spark.filodb.downsample.resolutions=1,5 \
  --conf spark.filodb.downsample.ttls=30,183 \
  spark-jobs-assembly-0.9-SNAPSHOT.jar
```

---

## Otros Jobs de Spark

### Label Churn Finder

Detecta series temporales con alta rotación de labels, lo cual puede causar problemas de cardinalidad.

```bash
spark-submit \
  --class filodb.downsampler.LabelChurnFinderMain \
  --master local[4] \
  spark-jobs-assembly-0.9-SNAPSHOT.jar
```

---

## Spark y FiloDB como Data Source (Legacy)

El módulo `spark/` (legacy) permite usar FiloDB como fuente de datos de Spark SQL:

```scala
// Lectura de datos desde FiloDB
val df = spark.read
  .format("filodb.spark")
  .option("dataset", "prometheus")
  .option("database", "filodb")
  .load()

// Consulta SQL
df.createTempView("metrics")
spark.sql("SELECT * FROM metrics WHERE _metric_ = 'cpu_usage' LIMIT 100")
```

> **Nota:** Este módulo es considerado legacy. El enfoque recomendado es usar la API HTTP con PromQL o el CLI.

---

## Diagrama de Flujo Completo

```
                    ┌─────────────────────────────────┐
                    │          Datos Raw               │
                    │    (resolución: 15-30s)          │
                    │    TTL: 24 horas                 │
                    └──────────────┬──────────────────┘
                                   │
                         Spark Job (cada 6h)
                                   │
                    ┌──────────────┼──────────────────┐
                    ▼                                  ▼
          ┌──────────────────┐             ┌──────────────────┐
          │  Downsampled 1m  │             │  Downsampled 5m  │
          │  TTL: 30 días    │             │  TTL: 183 días   │
          └──────────────────┘             └──────────────────┘
```

**Selección automática de resolución:**
El query engine de FiloDB selecciona automáticamente la resolución apropiada basándose en:
- El rango de tiempo de la consulta
- El step solicitado
- Un análisis costo-beneficio (datos raw vs. downsampled)
