# 11 — Comparativa: FiloDB vs Prometheus vs Grafana Mimir

## Visión General

FiloDB, Prometheus y Grafana Mimir son tres soluciones para el almacenamiento de series temporales, pero cada una tiene un enfoque diferente. Esta comparativa analiza las ventajas y desventajas de cada solución.

---

## Tabla Comparativa General

| Aspecto | FiloDB | Prometheus | Grafana Mimir |
|---------|--------|------------|---------------|
| **Tipo** | Base de datos distribuida | Servidor de monitoreo | Backend distribuido para Prometheus |
| **Arquitectura** | Distribuida (Akka Cluster) | Single-node | Distribuida (microservicios) |
| **Almacenamiento** | Cassandra + memoria off-heap | TSDB local (disco) | Object storage (S3, GCS, Azure) |
| **Lenguaje** | Scala/Java (JVM) | Go | Go |
| **Consultas** | PromQL | PromQL | PromQL |
| **Ingesta** | Kafka + Gateway | Pull (scraping) + Remote Write | Push (Remote Write) |
| **Escalabilidad** | Horizontal (sharding) | Vertical (limitada) | Horizontal (microservicios) |
| **Alta disponibilidad** | Sí (multi-DC) | Limitada (Thanos/Mimir) | Sí (nativa) |
| **Licencia** | Apache 2.0 | Apache 2.0 | AGPLv3 |

---

## FiloDB: Ventajas

### ✅ 1. Rendimiento en Tiempo Real
FiloDB almacena datos en **memoria off-heap**, lo que permite consultas de **sub-milisegundos** para datos recientes. No hay latencia de disco para datos activos.

### ✅ 2. Compresión Superior de Histogramas
FiloDB logra hasta **50x de compresión** en histogramas gracias a sus algoritmos especializados (NibblePacking, delta incremental). Prometheus almacena cada bucket como una serie separada.

```
Prometheus: 1 histograma con 20 buckets = 23 series (bucket, sum, count, +Inf)
FiloDB:     1 histograma con 20 buckets = 1 serie (formato nativo)
```

### ✅ 3. Soporte Nativo de OpenTelemetry
FiloDB tiene esquemas nativos para datos OTel:
- Histogramas acumulativos, delta y exponenciales
- Campos min/max dedicados
- Pre-agregación

Prometheus requiere conversión de formatos OTel → Prometheus, perdiendo información.

### ✅ 4. Multi-esquema
FiloDB soporta múltiples esquemas en el mismo cluster (gauge, counter, histogram, delta, OTel), cada uno optimizado para su tipo de dato.

### ✅ 5. Persistencia Durable con Cassandra
Los datos se persisten en Cassandra con replicación configurable, permitiendo **operación multi-datacenter** nativa.

### ✅ 6. Downsampling Integrado
FiloDB tiene downsampling integrado via Spark jobs, reduciendo automáticamente la resolución de datos históricos con algoritmos matemáticamente correctos.

### ✅ 7. Memoria Off-Heap
Evita pausas de garbage collection (GC) al usar memoria fuera del heap de JVM, crucial para consultas de baja latencia consistente.

### ✅ 8. Ingesta Basada en Kafka
La ingesta via Kafka proporciona:
- Buffer ante picos de tráfico
- Replay de datos tras fallos
- Desacoplamiento entre productores y consumidores
- Escalado independiente de ingesta y consulta

---

## FiloDB: Desventajas

### ❌ 1. Complejidad Operativa
FiloDB requiere gestionar múltiples componentes:
- Cluster de Cassandra
- Cluster de Kafka (+ ZooKeeper/KRaft)
- Cluster de FiloDB (nodos JVM)
- Spark para downsampling

**Prometheus:** Un solo binario.
**Mimir:** Más componentes que Prometheus, pero menos que FiloDB.

### ❌ 2. Consumo de Recursos
Al ser basado en JVM, FiloDB consume significativamente más memoria y CPU que soluciones en Go.

```
FiloDB: ~4-8 GB RAM por nodo (heap + off-heap)
Prometheus: ~1-4 GB RAM típicamente
Mimir: ~2-4 GB RAM por componente
```

### ❌ 3. Curva de Aprendizaje
El ecosistema Scala/Akka/SBT es menos familiar que Go para la mayoría de equipos DevOps/SRE.

### ❌ 4. Comunidad Más Pequeña
FiloDB tiene una comunidad significativamente más pequeña que Prometheus o Mimir:
- Menos documentación comunitaria
- Menos integraciones de terceros
- Menos soporte en foros

### ❌ 5. Sin Alerting Nativo
FiloDB no incluye un sistema de alertas propio. Necesitas Grafana Alerting, Alertmanager u otra herramienta.

**Prometheus:** Incluye Alertmanager.
**Mimir:** Compatible con Ruler (alertas remotas).

### ❌ 6. Sin Scraping Nativo
FiloDB no hace scraping de targets. Necesitas un componente externo (Prometheus, Telegraf, OTel Collector) para recopilar métricas.

### ❌ 7. Latencia de Cassandra para Datos Históricos
Las consultas sobre datos que ya no están en memoria requieren acceso a Cassandra, con latencias notablemente mayores que datos en memoria.

---

## Prometheus: Ventajas y Desventajas

### Ventajas de Prometheus

| Ventaja | Detalle |
|---------|---------|
| **Simplicidad** | Un solo binario, configuración YAML simple |
| **Scraping nativo** | Pull-based con service discovery automático |
| **Alertmanager** | Sistema de alertas integrado y maduro |
| **Ecosistema rico** | Miles de exporters, dashboards, integraciones |
| **Comunidad masiva** | CNCF Graduated, documentación excelente |
| **Bajo consumo** | Eficiente en Go, funciona con pocos recursos |
| **Estándar de facto** | El formato Prometheus es el estándar de métricas |

### Desventajas de Prometheus

| Desventaja | Detalle |
|------------|---------|
| **Escalabilidad limitada** | Single-node, no escala horizontalmente |
| **Sin alta disponibilidad nativa** | Requiere Thanos o Mimir para HA |
| **Retención limitada** | Almacenamiento local, no ideal para long-term |
| **Sin multi-tenancy** | Un Prometheus por equipo/entorno |
| **Cardinalidad limitada** | Problemas de rendimiento con alta cardinalidad |
| **Histogramas ineficientes** | Cada bucket es una serie separada |

---

## Grafana Mimir: Ventajas y Desventajas

### Ventajas de Mimir

| Ventaja | Detalle |
|---------|---------|
| **Escalabilidad horizontal** | Microservicios independientes |
| **Object storage** | S3, GCS, Azure — barato y duradero |
| **Multi-tenancy nativo** | Aislamiento completo entre tenants |
| **Compatible con Prometheus** | Drop-in replacement para remote write/read |
| **Alta disponibilidad** | Replicación de escritura, deduplicación |
| **Grafana integration** | Integración profunda con el stack Grafana |
| **Compactación eficiente** | Reduce costos de almacenamiento a largo plazo |

### Desventajas de Mimir

| Desventaja | Detalle |
|------------|---------|
| **Licencia AGPLv3** | Restrictiva para uso comercial SaaS |
| **Complejidad operativa** | Múltiples microservicios que gestionar |
| **Latencia de consulta** | Object storage es más lento que memoria |
| **Sin ingesta Kafka nativa** | Requiere Prometheus o OTel como frontend |
| **Costo de almacenamiento** | Object storage tiene costos de API |
| **Vendor lock-in (parcial)** | Fuertemente ligado al ecosistema Grafana |

---

## Comparativa por Caso de Uso

### Caso 1: Startup pequeña (< 100 servicios)

| Criterio | Recomendación |
|----------|---------------|
| **Mejor opción** | **Prometheus** |
| **Razón** | Simplicidad, bajo costo, funciona en un solo servidor |
| **FiloDB** | Excesivo para este volumen |
| **Mimir** | Innecesario para este tamaño |

### Caso 2: Empresa mediana (100-1000 servicios)

| Criterio | Recomendación |
|----------|---------------|
| **Mejor opción** | **Mimir** o **FiloDB** |
| **Mimir** | Si ya usas Grafana Cloud o quieres simplicidad relativa |
| **FiloDB** | Si necesitas histogramas nativos OTel o ya tienes Cassandra/Kafka |
| **Prometheus** | Ya no escala bien solo |

### Caso 3: Empresa grande (> 1000 servicios, multi-DC)

| Criterio | Recomendación |
|----------|---------------|
| **Mejor opción** | **FiloDB** o **Mimir** |
| **FiloDB** | Multi-DC nativo, Cassandra replication, Kafka buffer |
| **Mimir** | Object storage centralizado, multi-tenancy |
| **Prometheus** | Solo como scraper frontal |

### Caso 4: Necesidad de histogramas OTel nativos

| Criterio | Recomendación |
|----------|---------------|
| **Mejor opción** | **FiloDB** |
| **Razón** | Esquemas nativos OTel, compresión 50x |
| **Prometheus** | Convierte OTel a formato plano (pérdida de información) |
| **Mimir** | Hereda las limitaciones de Prometheus |

### Caso 5: Presupuesto limitado, máxima simplicidad

| Criterio | Recomendación |
|----------|---------------|
| **Mejor opción** | **Prometheus** |
| **Razón** | Gratis, un binario, mínima infraestructura |
| **FiloDB** | Requiere Cassandra + Kafka + JVM |
| **Mimir** | Requiere object storage + componentes |

---

## Tabla Resumen de Decisión

```
¿Necesitas...?                                    → Usa
─────────────────────────────────────────────────────────────
Simple monitoreo < 100 servicios                  → Prometheus
Escalabilidad horizontal + object storage barato  → Mimir
Multi-DC con Cassandra existente                  → FiloDB
Histogramas OTel nativos eficientes               → FiloDB
Multi-tenancy con aislamiento                     → Mimir
Mínima complejidad operativa                      → Prometheus
Buffer de ingesta (Kafka)                         → FiloDB
Integración profunda con Grafana                  → Mimir
Máxima baja latencia en datos recientes           → FiloDB
Alerting integrado                                → Prometheus
Retención a largo plazo barata                    → Mimir
```

---

## Arquitectura Comparada

```
Prometheus (simple):
┌──────────────┐
│  Prometheus  │──── scrape ────► Targets
│  (todo en 1) │
│  TSDB local  │◄─── PromQL ────  Grafana
└──────────────┘

Mimir (microservicios):
┌──────────┐  ┌───────────┐  ┌──────────┐  ┌─────────┐
│Distributor│─▶│ Ingester  │─▶│Compactor │─▶│ S3/GCS  │
└──────────┘  └───────────┘  └──────────┘  └─────────┘
                                                 ▲
┌──────────┐  ┌───────────┐                      │
│Query     │─▶│Store GW   │─────────────────────┘
│Frontend  │  └───────────┘
└──────────┘

FiloDB (distribuido):
┌──────────┐  ┌──────────┐  ┌──────────────────┐  ┌───────────┐
│ Gateway  │─▶│  Kafka   │─▶│ FiloDB Cluster   │─▶│ Cassandra │
└──────────┘  └──────────┘  │ (Akka, MemStore) │  └───────────┘
                             └────────┬─────────┘        │
                                      │              Spark (6h)
                             ┌────────▼─────────┐        │
                             │   HTTP Server     │  ┌─────▼─────┐
                             │   (PromQL API)    │  │Downsampled│
                             └──────────────────┘  └───────────┘
```
