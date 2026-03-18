# 13 — Ejemplos de Datos y Consultas

## Visión General

Este documento proporciona ejemplos prácticos de datos de entrada y consultas PromQL para FiloDB. Los ejemplos cubren diferentes tipos de métricas y escenarios de uso.

---

## Parte 1: Datos de Ejemplo para Ingestión

### 1.1 Formato Prometheus Remote Write

Los datos se envían típicamente desde Prometheus configurado con `remote_write`:

```yaml
# prometheus.yml
remote_write:
  - url: "http://filodb-gateway:9175/remote/write"

scrape_configs:
  - job_name: 'node_exporter'
    static_configs:
      - targets: ['server1:9100', 'server2:9100', 'server3:9100']
```

#### Métricas de ejemplo que Prometheus enviaría:

```
# Tipo: Gauge
node_memory_MemFree_bytes{instance="server1:9100",job="node_exporter"} 4294967296
node_memory_MemTotal_bytes{instance="server1:9100",job="node_exporter"} 17179869184
node_cpu_seconds_total{instance="server1:9100",job="node_exporter",cpu="0",mode="idle"} 123456.78

# Tipo: Counter
http_requests_total{method="GET",handler="/api/users",status="200",instance="app1:8080",job="web"} 54321
http_requests_total{method="POST",handler="/api/users",status="201",instance="app1:8080",job="web"} 1234
http_requests_total{method="GET",handler="/api/users",status="500",instance="app1:8080",job="web"} 42

# Tipo: Histogram
http_request_duration_seconds_bucket{le="0.005",method="GET",handler="/api",instance="app1:8080",job="web"} 24054
http_request_duration_seconds_bucket{le="0.01",method="GET",handler="/api",instance="app1:8080",job="web"} 33444
http_request_duration_seconds_bucket{le="0.025",method="GET",handler="/api",instance="app1:8080",job="web"} 100392
http_request_duration_seconds_bucket{le="0.05",method="GET",handler="/api",instance="app1:8080",job="web"} 129389
http_request_duration_seconds_bucket{le="0.1",method="GET",handler="/api",instance="app1:8080",job="web"} 133988
http_request_duration_seconds_bucket{le="+Inf",method="GET",handler="/api",instance="app1:8080",job="web"} 144320
http_request_duration_seconds_sum{method="GET",handler="/api",instance="app1:8080",job="web"} 53423.21
http_request_duration_seconds_count{method="GET",handler="/api",instance="app1:8080",job="web"} 144320
```

---

### 1.2 Formato InfluxDB Line Protocol

Para envío via el Gateway con protocolo InfluxDB:

```
# Formato: <measurement>,<tag_key>=<tag_value>,... <field_key>=<field_value>,... <timestamp_ns>

# Métricas de CPU
cpu_usage,host=server1,region=us-east,datacenter=dc1 value=0.85 1609461000000000000
cpu_usage,host=server2,region=us-east,datacenter=dc1 value=0.72 1609461000000000000
cpu_usage,host=server3,region=eu-west,datacenter=dc2 value=0.91 1609461000000000000

# Métricas de Memoria
memory_used_bytes,host=server1,region=us-east value=12884901888 1609461000000000000
memory_free_bytes,host=server1,region=us-east value=4294967296 1609461000000000000
memory_total_bytes,host=server1,region=us-east value=17179869184 1609461000000000000

# Métricas de Disco
disk_read_bytes,host=server1,device=sda value=1234567890 1609461000000000000
disk_write_bytes,host=server1,device=sda value=987654321 1609461000000000000
disk_usage_percent,host=server1,device=sda value=67.5 1609461000000000000

# Métricas de Red
network_receive_bytes,host=server1,interface=eth0 value=9876543210 1609461000000000000
network_transmit_bytes,host=server1,interface=eth0 value=1234567890 1609461000000000000

# Métricas de Aplicación
app_response_time_ms,service=api,endpoint=/users,method=GET value=45.2 1609461000000000000
app_response_time_ms,service=api,endpoint=/users,method=POST value=120.8 1609461000000000000
app_active_connections,service=api,instance=pod1 value=234 1609461000000000000
app_queue_depth,service=worker,queue=emails value=56 1609461000000000000
```

---

### 1.3 Datos OpenTelemetry

#### Gauge OTel (via OTLP → Collector → Remote Write)

```json
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "my-service"}},
        {"key": "host.name", "value": {"stringValue": "server1"}}
      ]
    },
    "scopeMetrics": [{
      "scope": {"name": "my-library", "version": "1.0"},
      "metrics": [{
        "name": "system.cpu.utilization",
        "unit": "1",
        "gauge": {
          "dataPoints": [{
            "attributes": [
              {"key": "cpu", "value": {"stringValue": "0"}},
              {"key": "state", "value": {"stringValue": "user"}}
            ],
            "timeUnixNano": "1609461000000000000",
            "asDouble": 0.85
          }]
        }
      }]
    }]
  }]
}
```

#### Histogram OTel (Delta)

```json
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "payment-service"}}
      ]
    },
    "scopeMetrics": [{
      "metrics": [{
        "name": "http.server.duration",
        "unit": "ms",
        "histogram": {
          "aggregationTemporality": 1,
          "dataPoints": [{
            "attributes": [
              {"key": "http.method", "value": {"stringValue": "POST"}},
              {"key": "http.route", "value": {"stringValue": "/api/payments"}}
            ],
            "startTimeUnixNano": "1609460940000000000",
            "timeUnixNano": "1609461000000000000",
            "count": "150",
            "sum": 4532.5,
            "min": 5.2,
            "max": 245.8,
            "bucketCounts": ["10", "25", "45", "35", "20", "10", "5"],
            "explicitBounds": [10, 25, 50, 100, 250, 500]
          }]
        }
      }]
    }]
  }]
}
```

#### Histogram OTel Exponencial (Delta)

```json
{
  "resourceMetrics": [{
    "scopeMetrics": [{
      "metrics": [{
        "name": "http.server.duration",
        "exponentialHistogram": {
          "aggregationTemporality": 1,
          "dataPoints": [{
            "timeUnixNano": "1609461000000000000",
            "count": "150",
            "sum": 4532.5,
            "min": 5.2,
            "max": 245.8,
            "scale": 3,
            "zeroCount": "0",
            "positive": {
              "offset": 5,
              "bucketCounts": ["1", "3", "8", "15", "25", "35", "30", "18", "10", "5"]
            }
          }]
        }
      }]
    }]
  }]
}
```

---

### 1.4 Datos de Ejemplo para Escenarios Específicos

#### Escenario: E-commerce

```
# Pedidos por segundo
orders_total,service=checkout,region=us-east,status=completed value=1523 1609461000000000000
orders_total,service=checkout,region=us-east,status=failed value=12 1609461000000000000
orders_total,service=checkout,region=eu-west,status=completed value=892 1609461000000000000

# Valor de pedidos
order_value_dollars,service=checkout,region=us-east,currency=USD value=45234.50 1609461000000000000
order_value_dollars,service=checkout,region=eu-west,currency=EUR value=28901.20 1609461000000000000

# Latencia de checkout
checkout_duration_ms,service=checkout,step=payment value=234.5 1609461000000000000
checkout_duration_ms,service=checkout,step=inventory value=45.2 1609461000000000000
checkout_duration_ms,service=checkout,step=shipping value=89.7 1609461000000000000

# Inventario
inventory_items,product=laptop,warehouse=us-east value=1523 1609461000000000000
inventory_items,product=laptop,warehouse=eu-west value=892 1609461000000000000
inventory_items,product=phone,warehouse=us-east value=5432 1609461000000000000
```

#### Escenario: Microservicios

```
# Requests por servicio
http_requests_total,service=auth,method=POST,status=200 value=45678 1609461000000000000
http_requests_total,service=auth,method=POST,status=401 value=234 1609461000000000000
http_requests_total,service=users,method=GET,status=200 value=123456 1609461000000000000
http_requests_total,service=users,method=GET,status=404 value=567 1609461000000000000
http_requests_total,service=orders,method=POST,status=201 value=8901 1609461000000000000
http_requests_total,service=orders,method=POST,status=500 value=23 1609461000000000000

# Conexiones activas
active_connections,service=auth,instance=pod-1 value=150 1609461000000000000
active_connections,service=auth,instance=pod-2 value=145 1609461000000000000
active_connections,service=users,instance=pod-1 value=320 1609461000000000000

# Cola de mensajes
queue_messages,queue=orders,status=pending value=456 1609461000000000000
queue_messages,queue=orders,status=processing value=23 1609461000000000000
queue_messages,queue=notifications,status=pending value=1234 1609461000000000000

# Latencias (como histogramas Prometheus)
request_duration_seconds_bucket{service="auth",le="0.01"} 12345
request_duration_seconds_bucket{service="auth",le="0.05"} 23456
request_duration_seconds_bucket{service="auth",le="0.1"} 34567
request_duration_seconds_bucket{service="auth",le="0.5"} 45678
request_duration_seconds_bucket{service="auth",le="1"} 45890
request_duration_seconds_bucket{service="auth",le="+Inf"} 45900
request_duration_seconds_sum{service="auth"} 2345.67
request_duration_seconds_count{service="auth"} 45900
```

---

## Parte 2: Consultas de Ejemplo (PromQL)

### 2.1 Consultas Básicas

#### Valor actual de una métrica

```bash
# Via HTTP
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query' \
  --data-urlencode 'query=up{job="node_exporter"}' \
  --data-urlencode 'time=1609461000'

# Via CLI
./filo-cli --host localhost --port 8080 --dataset prometheus \
  --promql 'up{job="node_exporter"}' \
  --start 1609461000 --step 15
```

#### Filtrar por labels

```promql
# Métrica con filtro exacto
http_requests_total{method="GET", status="200"}

# Filtro con regex
http_requests_total{status=~"5.."}

# Filtro negativo
http_requests_total{status!="200"}

# Filtro negativo con regex
http_requests_total{handler!~"/health.*"}
```

---

### 2.2 Funciones de Tasa (Rate)

#### Tasa de requests por segundo

```bash
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query_range' \
  --data-urlencode 'query=rate(http_requests_total{job="web"}[5m])' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609461000' \
  --data-urlencode 'step=60'
```

#### Tasa de errores

```promql
# Tasa de errores 5xx como porcentaje
100 * (
  rate(http_requests_total{status=~"5.."}[5m])
  /
  rate(http_requests_total[5m])
)
```

#### Incremento total en un período

```promql
# Total de requests en la última hora
increase(http_requests_total{job="web"}[1h])
```

---

### 2.3 Funciones de Agregación

#### Sum (Suma)

```promql
# Total de requests por servicio
sum by (service) (rate(http_requests_total[5m]))

# Total de requests por status
sum by (status) (rate(http_requests_total[5m]))
```

#### Avg (Promedio)

```promql
# Promedio de CPU por instancia
avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m]))

# Promedio general de uso de memoria
avg(node_memory_MemFree_bytes / node_memory_MemTotal_bytes)
```

#### Max / Min

```promql
# Máximo uso de CPU por host
max by (host) (cpu_usage)

# Mínima memoria libre
min(node_memory_MemFree_bytes)
```

#### Count

```promql
# Número de instancias activas
count(up{job="node_exporter"} == 1)

# Número de servicios con errores
count(rate(http_requests_total{status=~"5.."}[5m]) > 0)
```

#### TopK / BottomK

```promql
# Top 5 servicios con más requests
topk(5, sum by (service) (rate(http_requests_total[5m])))

# Bottom 3 hosts con menos memoria libre
bottomk(3, node_memory_MemFree_bytes)
```

---

### 2.4 Histogramas y Percentiles

#### Percentil 99 de latencia

```bash
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query_range' \
  --data-urlencode 'query=histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609461000' \
  --data-urlencode 'step=60'
```

#### Múltiples percentiles

```promql
# P50
histogram_quantile(0.5, rate(http_request_duration_seconds_bucket[5m]))

# P90
histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[5m]))

# P99
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))

# P99.9
histogram_quantile(0.999, rate(http_request_duration_seconds_bucket[5m]))
```

#### Percentil por servicio

```promql
histogram_quantile(0.95,
  sum by (service, le) (
    rate(http_request_duration_seconds_bucket[5m])
  )
)
```

#### Latencia promedio desde histograma

```promql
# Latencia promedio
rate(http_request_duration_seconds_sum[5m])
/
rate(http_request_duration_seconds_count[5m])
```

---

### 2.5 Operaciones Matemáticas

#### Porcentaje de uso de memoria

```promql
100 * (1 - (node_memory_MemFree_bytes / node_memory_MemTotal_bytes))
```

#### Uso de disco

```promql
100 * (
  (node_filesystem_size_bytes - node_filesystem_avail_bytes)
  / node_filesystem_size_bytes
)
```

#### Ratio de éxito

```promql
sum(rate(http_requests_total{status=~"2.."}[5m]))
/
sum(rate(http_requests_total[5m]))
* 100
```

---

### 2.6 Consultas Avanzadas

#### Alertas de SLA (Service Level Agreement)

```promql
# Porcentaje de requests exitosos en 30 minutos (SLI)
sum(rate(http_requests_total{status=~"2.."}[30m]))
/
sum(rate(http_requests_total[30m]))
* 100

# Alerta si SLI < 99.9%
sum(rate(http_requests_total{status=~"2.."}[30m]))
/
sum(rate(http_requests_total[30m]))
* 100
< 99.9
```

#### Detección de anomalías (cambios bruscos)

```promql
# Detectar cambio >50% en tasa de requests (vs hace 1h)
abs(
  rate(http_requests_total[5m])
  -
  rate(http_requests_total[5m] offset 1h)
)
/
rate(http_requests_total[5m] offset 1h)
> 0.5
```

#### Predicción de agotamiento de disco

```promql
# Predecir cuándo se agotará el disco (en horas)
node_filesystem_avail_bytes
/
(-deriv(node_filesystem_avail_bytes[1h]))
/ 3600
```

#### Tasa de errores por ventana deslizante

```promql
# Tasa de errores media móvil (30m)
avg_over_time(
  (
    sum(rate(http_requests_total{status=~"5.."}[5m]))
    /
    sum(rate(http_requests_total[5m]))
  )[30m:5m]
)
```

---

### 2.7 Consultas para Dashboards de Grafana

#### Panel: Tráfico General

```promql
# Requests por segundo (total)
sum(rate(http_requests_total[5m]))

# Requests por segundo (por servicio)
sum by (service) (rate(http_requests_total[5m]))
```

#### Panel: Latencias (RED Method)

```promql
# Rate
sum(rate(http_requests_total[5m]))

# Errors
sum(rate(http_requests_total{status=~"5.."}[5m]))

# Duration (P99)
histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))
```

#### Panel: USE Method (Utilization, Saturation, Errors)

```promql
# CPU Utilization
100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)

# Memory Saturation
node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes

# Disk Errors
rate(node_disk_io_time_weighted_seconds_total[5m])
```

#### Panel: Tabla de Instancias

```promql
# Estado de instancias con labels
up{job="node_exporter"}

# Uso de memoria por instancia (%)
100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))
```

---

### 2.8 Ejemplo Completo: Script de Consulta

```bash
#!/bin/bash
# script-consultas.sh - Ejemplos de consultas a FiloDB

FILODB_HOST="http://localhost:8080"
DATASET="prometheus"
END=$(date +%s)
START=$((END - 3600))  # Última hora
STEP=60

echo "=== Estado del Cluster ==="
curl -s "${FILODB_HOST}/api/v1/cluster/${DATASET}/status" | python3 -m json.tool

echo ""
echo "=== Labels Disponibles ==="
curl -s "${FILODB_HOST}/promql/${DATASET}/api/v1/labels" | python3 -m json.tool

echo ""
echo "=== Valores del Label 'job' ==="
curl -s "${FILODB_HOST}/promql/${DATASET}/api/v1/label/job/values" | python3 -m json.tool

echo ""
echo "=== Instancias Activas ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query" \
  --data-urlencode "query=count(up == 1)" \
  --data-urlencode "time=${END}" | python3 -m json.tool

echo ""
echo "=== Top 5 Métricas por Tasa ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query" \
  --data-urlencode "query=topk(5, sum by (__name__) (rate({__name__=~\".+\"}[5m])))" \
  --data-urlencode "time=${END}" | python3 -m json.tool

echo ""
echo "=== CPU Usage (último hora) ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query_range" \
  --data-urlencode "query=avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m]))" \
  --data-urlencode "start=${START}" \
  --data-urlencode "end=${END}" \
  --data-urlencode "step=${STEP}" | python3 -m json.tool

echo ""
echo "=== Tasa de Errores HTTP ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query_range" \
  --data-urlencode "query=sum(rate(http_requests_total{status=~\"5..\"}[5m])) / sum(rate(http_requests_total[5m])) * 100" \
  --data-urlencode "start=${START}" \
  --data-urlencode "end=${END}" \
  --data-urlencode "step=${STEP}" | python3 -m json.tool

echo ""
echo "=== Percentil 99 Latencia HTTP ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query_range" \
  --data-urlencode "query=histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))" \
  --data-urlencode "start=${START}" \
  --data-urlencode "end=${END}" \
  --data-urlencode "step=${STEP}" | python3 -m json.tool

echo ""
echo "=== Memoria Libre por Host ==="
curl -s -G "${FILODB_HOST}/promql/${DATASET}/api/v1/query" \
  --data-urlencode "query=node_memory_MemFree_bytes / 1024 / 1024 / 1024" \
  --data-urlencode "time=${END}" | python3 -m json.tool
```

---

## Parte 3: Respuestas Esperadas

### Formato de Respuesta: Vector (Instant Query)

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "up",
          "instance": "server1:9100",
          "job": "node_exporter"
        },
        "value": [1609461000, "1"]
      },
      {
        "metric": {
          "__name__": "up",
          "instance": "server2:9100",
          "job": "node_exporter"
        },
        "value": [1609461000, "1"]
      }
    ]
  }
}
```

### Formato de Respuesta: Matrix (Range Query)

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "http_requests_total",
          "method": "GET",
          "job": "web"
        },
        "values": [
          [1609459200, "23.5"],
          [1609459260, "24.1"],
          [1609459320, "22.8"],
          [1609459380, "25.3"],
          [1609459440, "21.9"],
          [1609459500, "26.7"]
        ]
      },
      {
        "metric": {
          "__name__": "http_requests_total",
          "method": "POST",
          "job": "web"
        },
        "values": [
          [1609459200, "5.2"],
          [1609459260, "4.8"],
          [1609459320, "6.1"],
          [1609459380, "5.5"],
          [1609459440, "4.9"],
          [1609459500, "7.3"]
        ]
      }
    ]
  }
}
```

### Formato de Respuesta: Scalar

```json
{
  "status": "success",
  "data": {
    "resultType": "scalar",
    "result": [1609461000, "42"]
  }
}
```
