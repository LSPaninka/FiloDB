# 📚 Documentación Exhaustiva de FiloDB

Bienvenido a la documentación completa del proyecto **FiloDB**, una base de datos distribuida, compatible con Prometheus, en tiempo real, en memoria, y masivamente escalable para series temporales y eventos.

---

## Índice de Contenidos

| #  | Documento | Descripción |
|----|-----------|-------------|
| 01 | [Introducción](01-introduccion.md) | Visión general del proyecto, casos de uso, características principales |
| 02 | [Módulos de la Aplicación](02-modulos.md) | Descripción detallada de cada módulo y sus relaciones |
| 03 | [Frameworks y Dependencias](03-frameworks-y-dependencias.md) | Tecnologías, librerías y frameworks utilizados |
| 04 | [Integración con Kafka](04-integracion-kafka.md) | Cómo FiloDB consume datos desde Apache Kafka |
| 05 | [Integración con Cassandra](05-integracion-cassandra.md) | Persistencia de datos, esquemas de tablas, recuperación |
| 06 | [Integración con Spark](06-integracion-spark.md) | Jobs de downsampling, procesamiento batch con Spark |
| 07 | [Filo-CLI](07-filo-cli.md) | Uso de la herramienta de línea de comandos |
| 08 | [Consultas HTTP](08-consultas-http.md) | API REST, endpoints Prometheus, administración |
| 09 | [Ingesta de Datos](09-ingesta-de-datos.md) | Mecanismos de ingestión: Kafka, Gateway, Remote Write |
| 10 | [OpenTelemetry](10-opentelemetry.md) | Integración OTel, esquemas soportados, formatos de datos |
| 11 | [Comparativa: Prometheus y Mimir](11-comparativa-prometheus-mimir.md) | Ventajas y desventajas frente a Prometheus y Grafana Mimir |
| 12 | [Despliegue: Local, Docker y Kubernetes](12-despliegue-local-docker-kubernetes.md) | Guía completa de instalación y despliegue |
| 13 | [Ejemplos de Datos y Consultas](13-ejemplos-datos-y-consultas.md) | Datos de ejemplo, consultas PromQL, casos prácticos |

---

## Arquitectura de Alto Nivel

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Prometheus     │     │   Telegraf       │     │   App Custom    │
│   Remote Write   │     │   InfluxDB Line  │     │   gRPC/HTTP     │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Gateway (Netty)                          │
│              Convierte a RecordContainer binario                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Apache Kafka                                │
│              Topic por dataset, particionado                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FiloDB Cluster (Akka)                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Nodo 0   │  │ Nodo 1   │  │ Nodo 2   │  │ Nodo N   │       │
│  │ Shards   │  │ Shards   │  │ Shards   │  │ Shards   │       │
│  │ 0,1      │  │ 2,3      │  │ 4,5      │  │ ...      │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              MemStore (Off-Heap Memory)                  │   │
│  │         Indexación Lucene + Compresión Columnar          │   │
│  └──────────────────────────┬──────────────────────────────┘   │
│                              │ Flush periódico                  │
└──────────────────────────────┼──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Cassandra                              │
│           Chunks comprimidos + Índices de partición             │
└─────────────────────────────┬───────────────────────────────────┘
                              │ Spark Jobs (cada 6h)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Cassandra (Downsampled)                         │
│         Resoluciones: 1min, 5min con TTLs extendidos            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Consultas

```
┌──────────────┐     ┌───────────────┐     ┌──────────────┐
│  Grafana     │     │  CLI (PromQL) │     │  HTTP Client │
└──────┬───────┘     └───────┬───────┘     └──────┬───────┘
       │                     │                     │
       ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│              FiloDB HTTP Server (Akka HTTP)                  │
│                                                             │
│  /promql/{dataset}/api/v1/query_range                       │
│  /promql/{dataset}/api/v1/query                             │
│  /promql/{dataset}/api/v1/labels                            │
│  /promql/{dataset}/api/v1/label/{name}/values               │
│  /api/v1/cluster/{dataset}/status                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Query Engine                                    │
│  PromQL → LogicalPlan → ExecPlan → RangeVectors            │
│  Selección basada en costos, poda de shards                 │
└─────────────────────────────────────────────────────────────┘
```

---

> Esta documentación fue creada para proporcionar una referencia exhaustiva del proyecto FiloDB, cubriendo desde la arquitectura hasta ejemplos prácticos de uso.
