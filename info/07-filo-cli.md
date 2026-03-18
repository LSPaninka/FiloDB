# 07 — Filo-CLI: Herramienta de Línea de Comandos

## Visión General

**Filo-CLI** es la herramienta de línea de comandos de FiloDB que permite gestionar datasets, ejecutar consultas PromQL, inspeccionar metadatos y depurar datos directamente desde la terminal.

---

## Instalación y Compilación

### Compilar el CLI

```bash
# Desde el directorio raíz de FiloDB
sbt cli/assembly
```

Esto genera un fat JAR en: `cli/target/scala-2.12/filo-cli-0.9-SNAPSHOT`

### Usar el Wrapper Script

```bash
# El script compila automáticamente si es necesario
./filo-cli --help
```

**Contenido del script `filo-cli`:**
```bash
#!/usr/bin/env bash
SCALA_VERSION="2.12"
FILO_VERSION=$(cat version.sbt | sed -e 's/.*"\(.*\)"/\1/g')
CLI_FILE=`pwd`"/cli/target/scala-$SCALA_VERSION/filo-cli-$FILO_VERSION"

if [ ! -f "$CLI_FILE" ]; then
  echo "Assembling CLI..."
  sbt cli/assembly
fi

java -cp "$CLI_FILE" filodb.cli.CliMain "$@"
```

---

## Opciones Generales

| Opción | Abreviatura | Descripción | Valor por Defecto |
|--------|-------------|-------------|-------------------|
| `--host` | `-h` | Host del coordinador | `localhost` |
| `--port` | `-p` | Puerto del coordinador | `2552` |
| `--command` | `-c` | Comando a ejecutar | (requerido) |
| `--dataset` | `-d` | Nombre del dataset | `prometheus` |
| `--database` | | Base de datos | `filodb` |
| `--limit` | `-l` | Límite de resultados | `200` |
| `--samplelimit` | | Límite de muestras | `1000000` |
| `--timeoutseconds` | | Timeout de consulta (s) | `60` |
| `--promql` | | Consulta PromQL | — |
| `--start` | | Timestamp inicio (epoch s) | — |
| `--end` | | Timestamp fin (epoch s) | — |
| `--step` | | Intervalo de paso (s) | — |
| `--spread` | | Spread para la consulta | — |
| `--shardkeyprefix` | | Filtro de shard key | — |

---

## Comandos Disponibles

### Gestión de Datasets

#### `init` — Inicializar metadatos
```bash
./filo-cli --command init --host localhost --port 2552
```

#### `list` — Listar datasets configurados
```bash
./filo-cli --command list --host localhost --port 2552
```

#### `clearMetadata` — Limpiar metadatos
```bash
./filo-cli --command clearMetadata
```

#### `validateSchemas` — Validar esquemas de datos
```bash
./filo-cli --command validateSchemas
```

---

### Consultas PromQL

#### Consulta de rango (range query)
```bash
# Consultar tasa de CPU en los últimos 30 minutos
./filo-cli \
  --host localhost \
  --port 8080 \
  --dataset prometheus \
  --promql 'rate(cpu_usage_total{instance="server1"}[5m])' \
  --start 1609459200 \
  --step 60 \
  --end 1609461000
```

#### Consulta instantánea (instant query)
```bash
# Consultar valor actual de memoria
./filo-cli \
  --host localhost \
  --port 8080 \
  --dataset prometheus \
  --promql 'memory_usage_bytes{job="node_exporter"}' \
  --start 1609461000 \
  --step 15
```

#### Consulta con funciones de agregación
```bash
# Promedio de CPU por instancia
./filo-cli \
  --host localhost \
  --port 8080 \
  --dataset prometheus \
  --promql 'avg by (instance)(rate(cpu_usage_total[5m]))' \
  --start 1609459200 \
  --step 60 \
  --end 1609461000
```

---

### Inspección de Metadatos

#### `indexnames` — Listar nombres de índices
```bash
./filo-cli --command indexnames \
  --host localhost --port 8080 \
  --dataset prometheus
```

#### `indexvalues` — Listar valores de un índice
```bash
./filo-cli --command indexvalues \
  --host localhost --port 8080 \
  --dataset prometheus \
  --indexname __name__
```

#### `labelvalues` — Valores de un label
```bash
./filo-cli --command labelvalues \
  --host localhost --port 8080 \
  --dataset prometheus \
  --labelnames job
```

#### `labels` — Listar todos los labels
```bash
./filo-cli --command labels \
  --host localhost --port 8080 \
  --dataset prometheus
```

---

### Cardinalidad

#### `labelcardinality` — Cardinalidad por label
```bash
./filo-cli --command labelcardinality \
  --host localhost --port 8080 \
  --dataset prometheus
```

#### `tscard` — Cardinalidad de series temporales
```bash
./filo-cli --command tscard \
  --host localhost --port 8080 \
  --dataset prometheus \
  --shardkeyprefix "job=myapp"
```

#### `topkcardlocal` — Top K por cardinalidad local
```bash
./filo-cli --command topkcardlocal \
  --host localhost --port 8080 \
  --dataset prometheus \
  --limit 10
```

---

### Depuración

#### `promFilterToPartKeyBR` — Convertir filtro PromQL a clave binaria
```bash
./filo-cli --command promFilterToPartKeyBR \
  --promql '{__name__="cpu_usage", job="node"}'
```

#### `partKeyBrAsString` — Mostrar clave de partición como string
```bash
./filo-cli --command partKeyBrAsString \
  --hexstring "0x..."
```

#### `decodeChunkInfo` — Decodificar metadatos de chunk
```bash
./filo-cli --command decodeChunkInfo \
  --hexstring "0x..."
```

#### `decodeVector` — Decodificar vector binario
```bash
./filo-cli --command decodeVector \
  --hexstring "0x..."
```

---

## Ejemplos Prácticos Completos

### 1. Flujo completo: Listar y consultar

```bash
# Paso 1: Verificar datasets disponibles
./filo-cli --command list --host localhost --port 8080

# Paso 2: Ver las métricas disponibles
./filo-cli --command indexvalues \
  --host localhost --port 8080 \
  --dataset prometheus \
  --indexname __name__

# Paso 3: Consultar una métrica específica
./filo-cli \
  --host localhost --port 8080 \
  --dataset prometheus \
  --promql 'up{job="node_exporter"}' \
  --start $(date -d '30 minutes ago' +%s) \
  --step 15 \
  --end $(date +%s)
```

### 2. Análisis de cardinalidad

```bash
# Ver labels con mayor cardinalidad
./filo-cli --command labelcardinality \
  --host localhost --port 8080 \
  --dataset prometheus

# Top 20 series por cardinalidad
./filo-cli --command topkcardlocal \
  --host localhost --port 8080 \
  --dataset prometheus \
  --limit 20
```

### 3. Consultas complejas con PromQL

```bash
# Percentil 99 de latencia HTTP
./filo-cli \
  --host localhost --port 8080 \
  --dataset prometheus \
  --promql 'histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))' \
  --start 1609459200 \
  --step 60 \
  --end 1609461000

# Tasa de errores HTTP
./filo-cli \
  --host localhost --port 8080 \
  --dataset prometheus \
  --promql 'sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))' \
  --start 1609459200 \
  --step 60 \
  --end 1609461000
```

---

## Formato de Salida

El CLI muestra los resultados en formato tabular:

```
Timestamp            | Value
─────────────────────┼──────────
2021-01-01T00:00:00  | 0.85
2021-01-01T00:01:00  | 0.92
2021-01-01T00:02:00  | 0.78
2021-01-01T00:03:00  | 0.95
...

Labels: {__name__="cpu_usage", instance="server1", job="node"}
```

---

## Conexión al Cluster

El CLI puede conectarse de dos maneras:

### 1. Conexión directa Akka (puerto 2552)
```bash
# Para comandos de gestión
./filo-cli --command list --host localhost --port 2552
```

### 2. Conexión HTTP (puerto 8080)
```bash
# Para consultas PromQL y metadatos
./filo-cli --host localhost --port 8080 --promql 'up'
```

---

## Notas Importantes

1. **El CLI requiere que el servidor FiloDB esté en ejecución**
2. **Las consultas PromQL usan timestamps en epoch seconds** (no milisegundos)
3. **El `step` define la resolución de los resultados** — usa valores más grandes para rangos largos
4. **El `spread`** controla cuántos shards se consultan — ajústalo según la distribución de datos
5. **El `samplelimit`** previene consultas que retornen demasiados datos — aumenta con precaución
