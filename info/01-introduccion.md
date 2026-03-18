# 01 — Introducción a FiloDB

## ¿Qué es FiloDB?

**FiloDB** es una base de datos distribuida, en tiempo real, en memoria y masivamente escalable, diseñada específicamente para **series temporales** y **eventos**. Es compatible con **Prometheus** a través de PromQL y está optimizada para consultas de baja latencia, dashboards y alertas.

FiloDB fue creado para manejar escenarios donde se necesita:

- Ingestar millones de series temporales por segundo
- Consultar datos en tiempo real con latencias de milisegundos
- Escalar horizontalmente a través de múltiples nodos
- Persistir datos de manera durable en Apache Cassandra
- Soportar consultas PromQL idénticas a las de Prometheus

## Características Principales

| Característica | Descripción |
|----------------|-------------|
| **Masivamente escalable** | Millones de entidades particionadas (shards) entre procesos |
| **Compatible con PromQL** | Soporte nativo del lenguaje de consultas de Prometheus |
| **Indexación por etiquetas** | Búsqueda eficiente por tags usando Apache Lucene |
| **Compresión columnar** | Delta-delta, XOR, NibblePacking — hasta 50x de ahorro en histogramas |
| **Baja latencia** | Diseñado para dashboards y alertas en tiempo real |
| **Disponibilidad en tiempo real** | Los datos están disponibles para consulta inmediatamente después de la ingesta |
| **Tolerante a fallos** | Operación en múltiples data centers |
| **Multi-esquema** | Soporte para gauges, counters, histogramas, delta-counters, OpenTelemetry |
| **Multi-stream** | Múltiples flujos de datos con diferentes esquemas en el mismo cluster |
| **Memoria off-heap** | Gestión de memoria fuera del heap de JVM para evitar GC pauses |

## Casos de Uso Ideales

### ✅ Para qué usar FiloDB

1. **Métricas operacionales en tiempo real**
   - Monitoreo de infraestructura y aplicaciones
   - Dashboards de Grafana con datos en vivo
   - Alertas basadas en series temporales

2. **Almacenamiento de trazas distribuidas**
   - Ingestión de spans de OpenTelemetry
   - Correlación de trazas con métricas

3. **Depuración ad-hoc**
   - Consultas interactivas sobre datos recientes
   - Análisis de incidentes en tiempo real

4. **Eventos en tiempo real**
   - Procesamiento de eventos de negocio
   - Monitoreo de flujos de datos

### ❌ Para qué NO usar FiloDB

1. **Cargas transaccionales pesadas** — No es un reemplazo de PostgreSQL o MySQL
2. **OLAP/Analytics** — No está optimizado para consultas analíticas complejas sobre datos históricos masivos
3. **Almacenamiento de documentos** — No es MongoDB ni Elasticsearch
4. **Datos no temporales** — Está diseñado específicamente para datos con marca de tiempo

## Versión Actual

```
Versión: 0.9-SNAPSHOT
Scala: 2.12.12
Java: 11+
```

## Requisitos Previos

| Componente | Versión Mínima | Propósito |
|------------|----------------|-----------|
| Java JDK | 11 | Entorno de ejecución |
| SBT | 1.x | Sistema de build (Scala) |
| Cassandra | 2.x / 3.x | Persistencia de datos |
| Kafka | 0.10+ | Ingesta de datos en streaming |
| Rust + C compiler | Última estable | Compilación de componentes nativos (vectores binarios) |

## Flujo de Datos General

```
Fuentes de Datos                 Ingesta               Almacenamiento        Consulta
─────────────────               ────────              ────────────────       ─────────
                                                                             
Prometheus    ──┐                                                           
               │    ┌──────────┐    ┌────────┐    ┌─────────────┐    ┌──────────┐
Telegraf     ──┼──▶ │ Gateway  │──▶ │ Kafka  │──▶ │  MemStore   │◀── │ PromQL   │
               │    └──────────┘    └────────┘    │ (Off-Heap)  │    │ HTTP/CLI │
App Custom   ──┘                                  └──────┬──────┘    └──────────┘
                                                         │                       
                                                         ▼                       
                                                  ┌─────────────┐               
                                                  │  Cassandra   │               
                                                  │  (Persist)   │               
                                                  └──────┬──────┘               
                                                         │ Spark (6h)           
                                                         ▼                       
                                                  ┌─────────────┐               
                                                  │ Downsampled  │               
                                                  │  Cassandra   │               
                                                  └─────────────┘               
```

## Licencia

FiloDB es un proyecto open-source distribuido bajo la licencia **Apache 2.0**.
