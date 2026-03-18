# 08 — Consultas HTTP (API REST)

## Visión General

FiloDB expone una **API REST compatible con Prometheus** a través de un servidor Akka HTTP. Esta API permite ejecutar consultas PromQL, gestionar el cluster, obtener metadatos y monitorear la salud del sistema.

**Puerto por defecto:** `8080` (configurable en `filodb.http.bind-port`)

---

## Configuración del Servidor HTTP

```hocon
# http/src/main/resources/reference.conf
filodb.http {
  bind-host = "127.0.0.1"   # Dirección de escucha
  bind-port = 8080           # Puerto HTTP
  start-timeout = 1 minute   # Timeout de inicio
  runtime-routes = []        # Rutas adicionales dinámicas
}
```

---

## Endpoints de Consulta Prometheus

### 1. Range Query — Consulta de Rango

**Endpoint:** `GET /promql/{dataset}/api/v1/query_range`

Ejecuta una consulta PromQL sobre un rango de tiempo. Es el endpoint más utilizado para dashboards de Grafana.

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `query` | string | ✅ | Expresión PromQL |
| `start` | number | ✅ | Timestamp de inicio (epoch seconds) |
| `end` | number | ✅ | Timestamp de fin (epoch seconds) |
| `step` | number | ✅ | Intervalo entre puntos de datos (seconds) |
| `explainOnly` | boolean | ❌ | Solo retorna el plan de ejecución |
| `verbose` | boolean | ❌ | Incluye información detallada de ejecución |
| `histogramMap` | boolean | ❌ | Retorna histogramas como mapas |
| `spread` | number | ❌ | Override del spread de shards |
| `queryTimeoutSecs` | number | ❌ | Timeout de la consulta |

**Ejemplo:**
```bash
# Tasa de requests HTTP en los últimos 30 minutos, con puntos cada 60s
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query_range' \
  --data-urlencode 'query=rate(http_requests_total[5m])' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609461000' \
  --data-urlencode 'step=60'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "http_requests_total",
          "instance": "server1:9090",
          "job": "web"
        },
        "values": [
          [1609459200, "23.5"],
          [1609459260, "24.1"],
          [1609459320, "22.8"]
        ]
      }
    ]
  }
}
```

---

### 2. Instant Query — Consulta Instantánea

**Endpoint:** `GET /promql/{dataset}/api/v1/query`

Ejecuta una consulta PromQL en un instante de tiempo específico.

**Parámetros:**

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `query` | string | ✅ | Expresión PromQL |
| `time` | number | ✅ | Timestamp del instante (epoch seconds) |
| `step` | number | ❌ | Intervalo de evaluación |
| `spread` | number | ❌ | Override del spread |

**Ejemplo:**
```bash
# Valor actual de memoria libre
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query' \
  --data-urlencode 'query=node_memory_MemFree_bytes' \
  --data-urlencode 'time=1609461000'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "node_memory_MemFree_bytes",
          "instance": "server1:9100",
          "job": "node"
        },
        "value": [1609461000, "4294967296"]
      }
    ]
  }
}
```

---

### 3. Labels — Nombres de Etiquetas

**Endpoint:** `GET /promql/{dataset}/api/v1/labels`

Retorna todos los nombres de labels disponibles.

**Ejemplo:**
```bash
curl 'http://localhost:8080/promql/prometheus/api/v1/labels'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": [
    "__name__",
    "instance",
    "job",
    "method",
    "status",
    "handler",
    "le"
  ]
}
```

---

### 4. Label Values — Valores de una Etiqueta

**Endpoint:** `GET /promql/{dataset}/api/v1/label/{labelName}/values`

Retorna todos los valores de un label específico.

**Ejemplo:**
```bash
# Obtener todos los valores del label "job"
curl 'http://localhost:8080/promql/prometheus/api/v1/label/job/values'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": [
    "node_exporter",
    "prometheus",
    "grafana",
    "my_app"
  ]
}
```

---

### 5. Remote Read — Lectura Remota de Prometheus

**Endpoint:** `POST /promql/{dataset}/api/v1/read`

Soporte del protocolo de lectura remota de Prometheus (protobuf + Snappy).

**Uso:** Se configura en Prometheus como `remote_read`:

```yaml
# prometheus.yml
remote_read:
  - url: "http://filodb:8080/promql/prometheus/api/v1/read"
    read_recent: true
```

---

## Endpoints de Gestión del Cluster

### 6. Listar Datasets

**Endpoint:** `GET /api/v1/cluster`

```bash
curl 'http://localhost:8080/api/v1/cluster'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": ["prometheus"]
}
```

---

### 7. Estado de Shards

**Endpoint:** `GET /api/v1/cluster/{dataset}/status`

```bash
curl 'http://localhost:8080/api/v1/cluster/prometheus/status'
```

**Respuesta:**
```json
{
  "status": "success",
  "data": [
    {
      "shard": 0,
      "status": "Active",
      "address": "akka://filo-standalone@127.0.0.1:2552",
      "numPartitions": 15234,
      "activelyScraping": 15234,
      "numChunks": 45702
    },
    {
      "shard": 1,
      "status": "Active",
      "address": "akka://filo-standalone@127.0.0.1:2552",
      "numPartitions": 14897,
      "activelyScraping": 14897,
      "numChunks": 44691
    }
  ]
}
```

---

### 8. Estado por Dirección de Nodo

**Endpoint:** `GET /api/v1/cluster/{dataset}/statusByAddress`

```bash
curl 'http://localhost:8080/api/v1/cluster/prometheus/statusByAddress'
```

Retorna los shards agrupados por nodo del cluster.

---

### 9. Iniciar Ingestión

**Endpoint:** `POST /api/v1/cluster/{dataset}`

Configura e inicia la ingestión de datos para un dataset.

```bash
curl -X POST 'http://localhost:8080/api/v1/cluster/prometheus' \
  -H 'Content-Type: application/json' \
  -d @conf/timeseries-dev-source.conf
```

---

### 10. Detener Shards

**Endpoint:** `POST /api/v1/cluster/{dataset}/stopshards`

```bash
curl -X POST 'http://localhost:8080/api/v1/cluster/prometheus/stopshards' \
  -H 'Content-Type: application/json' \
  -d '{"shards": [0, 1]}'
```

---

### 11. Iniciar Shards

**Endpoint:** `POST /api/v1/cluster/{dataset}/startshards`

```bash
curl -X POST 'http://localhost:8080/api/v1/cluster/prometheus/startshards' \
  -H 'Content-Type: application/json' \
  -d '{"shards": [0, 1]}'
```

---

## Endpoints de Administración

### 12. Health Check

**Endpoint:** `GET /admin/health`

```bash
curl 'http://localhost:8080/admin/health'
```

**Respuesta:**
```json
{
  "status": "healthy",
  "uptime": "2h 30m",
  "version": "0.9-SNAPSHOT"
}
```

---

### 13. Descubrimiento de Miembros

**Endpoint:** `GET /__members`

Usado por el `akka-bootstrapper` para descubrimiento de seeds.

```bash
curl 'http://localhost:8080/__members'
```

**Respuesta:**
```json
{
  "members": [
    "akka://filo-standalone@127.0.0.1:2552",
    "akka://filo-standalone@127.0.0.1:2553"
  ]
}
```

---

### 14. Cambiar Nivel de Log

**Endpoint:** `POST /admin/loglevel/{loggerName}`

```bash
# Cambiar el nivel de log de la ingesta a DEBUG
curl -X POST 'http://localhost:8080/admin/loglevel/filodb.coordinator.IngestionActor' \
  -H 'Content-Type: application/json' \
  -d '{"level": "DEBUG"}'
```

---

## Integración con Grafana

FiloDB es compatible como **datasource de Prometheus** en Grafana:

### Configuración en Grafana

1. **Agregar datasource** → Tipo: Prometheus
2. **URL:** `http://filodb-host:8080/promql/prometheus`
3. **Access:** Server (proxy) o Browser (directo)

```
URL del datasource: http://filodb:8080/promql/prometheus
```

### Ejemplo de dashboard query
```promql
# Panel de CPU
rate(cpu_usage_total{instance=~"$instance"}[5m])

# Panel de Memoria
node_memory_MemTotal_bytes - node_memory_MemFree_bytes

# Panel de Red
rate(node_network_receive_bytes_total{device="eth0"}[5m])
```

---

## Manejo de Errores

### Códigos de Estado HTTP

| Código | Significado |
|--------|-------------|
| `200` | Consulta exitosa |
| `400` | Consulta malformada (error de PromQL) |
| `408` | Timeout de la consulta |
| `422` | Parámetros inválidos |
| `500` | Error interno del servidor |
| `503` | Servicio no disponible (shards no activos) |

### Ejemplo de error
```json
{
  "status": "error",
  "errorType": "bad_data",
  "error": "parse error: unexpected character in PromQL expression"
}
```

---

## Resumen de Endpoints

| Método | Endpoint | Propósito |
|--------|----------|-----------|
| GET | `/promql/{ds}/api/v1/query_range` | Consulta de rango PromQL |
| GET | `/promql/{ds}/api/v1/query` | Consulta instantánea PromQL |
| GET | `/promql/{ds}/api/v1/labels` | Nombres de labels |
| GET | `/promql/{ds}/api/v1/label/{name}/values` | Valores de un label |
| POST | `/promql/{ds}/api/v1/read` | Lectura remota Prometheus |
| GET | `/api/v1/cluster` | Listar datasets |
| GET | `/api/v1/cluster/{ds}/status` | Estado de shards |
| GET | `/api/v1/cluster/{ds}/statusByAddress` | Estado por nodo |
| POST | `/api/v1/cluster/{ds}` | Iniciar ingestión |
| POST | `/api/v1/cluster/{ds}/stopshards` | Detener shards |
| POST | `/api/v1/cluster/{ds}/startshards` | Iniciar shards |
| GET | `/admin/health` | Health check |
| GET | `/__members` | Descubrimiento de seeds |
| POST | `/admin/loglevel/{logger}` | Cambiar nivel de log |
