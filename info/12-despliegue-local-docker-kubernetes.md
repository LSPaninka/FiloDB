# 12 — Despliegue: Local, Docker y Kubernetes

## Requisitos Previos Generales

| Componente | Versión Mínima | Notas |
|------------|----------------|-------|
| Java JDK | 11+ | Oracle o OpenJDK |
| SBT | 1.x | Para compilar desde fuente |
| Apache Cassandra | 2.x / 3.x | Base de datos de persistencia |
| Apache Kafka | 0.10+ | Sistema de ingestión |
| Rust + C compiler | Última estable | Para componentes nativos |
| Docker | 20.10+ | Para despliegue containerizado |
| Kubernetes | 1.21+ | Para despliegue en cluster |
| kubectl | 1.21+ | CLI de Kubernetes |
| Helm | 3.x | Opcional, para charts |

---

## Parte 1: Despliegue Local (Desarrollo)

### Paso 1: Instalar Dependencias

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk curl unzip

# Instalar SBT
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install -y sbt

# Instalar Rust (para componentes nativos)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# macOS (con Homebrew)
brew install openjdk@11 sbt rust
```

### Paso 2: Instalar Cassandra

```bash
# Opción A: Cassandra local
# Descargar desde https://cassandra.apache.org/download/
wget https://downloads.apache.org/cassandra/3.11.16/apache-cassandra-3.11.16-bin.tar.gz
tar xzf apache-cassandra-3.11.16-bin.tar.gz
cd apache-cassandra-3.11.16

# Iniciar Cassandra
bin/cassandra -f

# Verificar
bin/cqlsh -e "DESCRIBE KEYSPACES;"

# Opción B: Cassandra via Docker
docker run -d --name cassandra \
  -p 9042:9042 \
  cassandra:3.11
```

### Paso 3: Instalar Kafka

```bash
# Opción A: Kafka local
wget https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz
tar xzf kafka_2.13-3.6.2.tgz
cd kafka_2.13-3.6.2

# Iniciar ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Iniciar Kafka
bin/kafka-server-start.sh config/server.properties &

# Crear topic
bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 4 \
  --topic timeseries-dev

# Opción B: Kafka via Docker
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.5.0
```

### Paso 4: Compilar FiloDB

```bash
# Clonar el repositorio
git clone https://github.com/filodb/FiloDB.git
cd FiloDB

# Compilar los componentes principales
sbt standalone/assembly     # Servidor standalone
sbt cli/assembly            # Herramienta CLI
sbt gateway/assembly        # Gateway de ingestión
```

### Paso 5: Crear Esquema en Cassandra

```bash
# Generar DDL
./scripts/schema-create.sh filodb_admin filodb filodb_downsample prometheus 4 1,5 > /tmp/ddl.cql

# Ejecutar DDL
cqlsh -f /tmp/ddl.cql

# Verificar
cqlsh -e "DESCRIBE KEYSPACE filodb;"
```

### Paso 6: Iniciar el Servidor FiloDB

```bash
# Usando el script de desarrollo
./filodb-dev-start.sh -o 0

# O manualmente
java -Xmx2G \
  -Dconfig.file=conf/timeseries-filodb-server.conf \
  -cp standalone/target/scala-2.12/standalone-assembly-0.9-SNAPSHOT.jar \
  filodb.standalone.FiloServer
```

**El servidor estará disponible en:**
- HTTP API: `http://localhost:8080`
- Akka Cluster: `localhost:2552`
- gRPC: `localhost:8888` (si está habilitado)

### Paso 7: Configurar Ingesta

```bash
# Opción A: Via HTTP API
curl -X POST 'http://localhost:8080/api/v1/cluster/prometheus' \
  -H 'Content-Type: application/json' \
  -d @conf/timeseries-dev-source.conf

# Opción B: La configuración se puede incluir en el archivo del servidor
# para auto-inicio de ingesta
```

### Paso 8: Verificar

```bash
# Health check
curl http://localhost:8080/admin/health

# Estado de shards
curl http://localhost:8080/api/v1/cluster/prometheus/status

# Consulta de prueba
curl -G 'http://localhost:8080/promql/prometheus/api/v1/query' \
  --data-urlencode 'query=up' \
  --data-urlencode 'time='$(date +%s)
```

### Cluster Multi-nodo Local

Para ejecutar un cluster de 2 nodos localmente:

```bash
# Terminal 1: Nodo 0
./filodb-dev-start.sh -o 0

# Terminal 2: Nodo 1
./filodb-dev-start.sh -o 1
```

El script configura automáticamente puertos diferentes para cada nodo:
- Nodo 0: HTTP 8080, Akka 2552
- Nodo 1: HTTP 8081, Akka 2553

---

## Parte 2: Despliegue con Docker

### Dockerfile para FiloDB

```dockerfile
# Dockerfile
FROM openjdk:11-jre-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no-root
RUN groupadd -r filodb && useradd -r -g filodb filodb

# Copiar el JAR standalone
COPY standalone/target/scala-2.12/standalone-assembly-*.jar /opt/filodb/filodb.jar

# Copiar configuraciones
COPY conf/ /opt/filodb/conf/
COPY scripts/ /opt/filodb/scripts/

# Directorio de trabajo
WORKDIR /opt/filodb

# Puerto HTTP
EXPOSE 8080
# Puerto Akka
EXPOSE 2552
# Puerto gRPC
EXPOSE 8888

# Usuario
USER filodb

# JVM options
ENV JAVA_OPTS="-Xmx2G -XX:+UseG1GC -XX:MaxDirectMemorySize=2G"

# Entrypoint
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Dconfig.file=conf/timeseries-filodb-server.conf -cp filodb.jar filodb.standalone.FiloServer"]
```

### Docker Compose: Stack Completo

```yaml
# docker-compose.yml
version: '3.8'

services:
  # === Cassandra ===
  cassandra:
    image: cassandra:3.11
    container_name: filodb-cassandra
    ports:
      - "9042:9042"
    volumes:
      - cassandra-data:/var/lib/cassandra
    environment:
      - CASSANDRA_CLUSTER_NAME=filodb
      - MAX_HEAP_SIZE=1G
      - HEAP_NEWSIZE=256M
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'describe cluster'"]
      interval: 30s
      timeout: 10s
      retries: 5

  # === Cassandra Schema Init ===
  cassandra-init:
    image: cassandra:3.11
    depends_on:
      cassandra:
        condition: service_healthy
    volumes:
      - ./scripts:/scripts
    entrypoint: >
      bash -c "
        /scripts/schema-create.sh filodb_admin filodb filodb_downsample prometheus 4 1,5 > /tmp/ddl.cql &&
        cqlsh cassandra -f /tmp/ddl.cql &&
        echo 'Schema created successfully'
      "

  # === Zookeeper ===
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: filodb-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  # === Kafka ===
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: filodb-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list"]
      interval: 30s
      timeout: 10s
      retries: 5

  # === Kafka Topic Init ===
  kafka-init:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: >
      bash -c "
        kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 4 --topic timeseries-dev &&
        echo 'Kafka topic created successfully'
      "

  # === FiloDB Server ===
  filodb:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: filodb-server
    depends_on:
      cassandra-init:
        condition: service_completed_successfully
      kafka-init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
      - "2552:2552"
    environment:
      JAVA_OPTS: "-Xmx2G -XX:+UseG1GC -XX:MaxDirectMemorySize=2G"
      CASSANDRA_HOSTS: "cassandra"
      KAFKA_BOOTSTRAP: "kafka:29092"
    volumes:
      - ./conf:/opt/filodb/conf

  # === Grafana (opcional) ===
  grafana:
    image: grafana/grafana:10.0.0
    container_name: filodb-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana

volumes:
  cassandra-data:
  grafana-data:
```

### Comandos Docker Compose

```bash
# Construir e iniciar todo el stack
docker compose up -d --build

# Ver logs
docker compose logs -f filodb

# Verificar estado
curl http://localhost:8080/admin/health

# Detener todo
docker compose down

# Detener y eliminar datos
docker compose down -v
```

### Configuración Docker del Servidor

```hocon
# conf/timeseries-filodb-server-docker.conf
filodb {
  cassandra {
    hosts = ${?CASSANDRA_HOSTS}  # Variable de entorno
    port = 9042
  }
}

sourceconfig {
  filo-topic-name = "timeseries-dev"
  bootstrap.servers = ${?KAFKA_BOOTSTRAP}  # Variable de entorno
}
```

---

## Parte 3: Despliegue en Kubernetes (Multi-nodo)

### Arquitectura en Kubernetes

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                         │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Namespace: filodb                                    │   │
│  │                                                       │   │
│  │  ┌─────────────────┐  ┌─────────────────┐           │   │
│  │  │ StatefulSet:     │  │ StatefulSet:     │           │   │
│  │  │ filodb-server    │  │ cassandra        │           │   │
│  │  │ Replicas: 3      │  │ Replicas: 3      │           │   │
│  │  │                  │  │                  │           │   │
│  │  │ filodb-server-0  │  │ cassandra-0      │           │   │
│  │  │ filodb-server-1  │  │ cassandra-1      │           │   │
│  │  │ filodb-server-2  │  │ cassandra-2      │           │   │
│  │  └─────────────────┘  └─────────────────┘           │   │
│  │                                                       │   │
│  │  ┌─────────────────┐  ┌─────────────────┐           │   │
│  │  │ StatefulSet:     │  │ Service:         │           │   │
│  │  │ kafka            │  │ filodb-http      │           │   │
│  │  │ Replicas: 3      │  │ (LoadBalancer)   │           │   │
│  │  │                  │  │ Port: 8080       │           │   │
│  │  │ kafka-0          │  └─────────────────┘           │   │
│  │  │ kafka-1          │                                 │   │
│  │  │ kafka-2          │  ┌─────────────────┐           │   │
│  │  └─────────────────┘  │ Service:         │           │   │
│  │                        │ filodb-headless  │           │   │
│  │                        │ (ClusterIP:None) │           │   │
│  │                        │ Para discovery   │           │   │
│  │                        └─────────────────┘           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Namespace

```yaml
# k8s/00-namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: filodb
```

### ConfigMap

```yaml
# k8s/01-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: filodb-config
  namespace: filodb
data:
  filodb-server.conf: |
    filodb {
      cassandra {
        hosts = "cassandra-0.cassandra.filodb.svc.cluster.local,cassandra-1.cassandra.filodb.svc.cluster.local,cassandra-2.cassandra.filodb.svc.cluster.local"
        port = 9042
        keyspace = "filodb"
        admin-keyspace = "filodb_admin"
        downsample-keyspace = "filodb_downsample"
        read-consistency = "LOCAL_QUORUM"
        write-consistency = "LOCAL_QUORUM"
      }
      
      http {
        bind-host = "0.0.0.0"
        bind-port = 8080
      }
      
      cluster-discovery {
        # Descubrimiento por Kubernetes StatefulSet
        k8s-stateful-sets-hostname-format = "filodb-server-{}.filodb-headless.filodb.svc.cluster.local"
      }
    }
    
    sourceconfig {
      filo-topic-name = "timeseries-prod"
      bootstrap.servers = "kafka-0.kafka-headless.filodb.svc.cluster.local:9092,kafka-1.kafka-headless.filodb.svc.cluster.local:9092,kafka-2.kafka-headless.filodb.svc.cluster.local:9092"
      group.id = "filodb-ingestion"
      
      store {
        flush-interval = 1h
        disk-time-to-live = 72 hours
        shard-mem-size = 1GB
        ingestion-buffer-mem-size = 512MB
        groups-per-shard = 60
      }
    }
  
  source.conf: |
    dataset = "prometheus"
    schema = "prom-counter"
    num-shards = 12
    min-num-nodes = 3
    sourcefactory = "filodb.kafka.KafkaIngestionStreamFactory"
```

### Servicio Headless (para descubrimiento)

```yaml
# k8s/02-service-headless.yaml
apiVersion: v1
kind: Service
metadata:
  name: filodb-headless
  namespace: filodb
  labels:
    app: filodb
spec:
  clusterIP: None
  selector:
    app: filodb
  ports:
    - name: http
      port: 8080
      targetPort: 8080
    - name: akka
      port: 2552
      targetPort: 2552
    - name: grpc
      port: 8888
      targetPort: 8888
```

### Servicio LoadBalancer

```yaml
# k8s/03-service-lb.yaml
apiVersion: v1
kind: Service
metadata:
  name: filodb-http
  namespace: filodb
  labels:
    app: filodb
spec:
  type: LoadBalancer
  selector:
    app: filodb
  ports:
    - name: http
      port: 8080
      targetPort: 8080
```

### StatefulSet de FiloDB

```yaml
# k8s/04-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: filodb-server
  namespace: filodb
spec:
  serviceName: filodb-headless
  replicas: 3
  selector:
    matchLabels:
      app: filodb
  template:
    metadata:
      labels:
        app: filodb
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: filodb
          image: your-registry/filodb:0.9
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 2552
              name: akka
            - containerPort: 8888
              name: grpc
          env:
            - name: JAVA_OPTS
              value: "-Xmx4G -XX:+UseG1GC -XX:MaxDirectMemorySize=4G"
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: config
              mountPath: /opt/filodb/conf
          resources:
            requests:
              memory: "6Gi"
              cpu: "2"
            limits:
              memory: "8Gi"
              cpu: "4"
          readinessProbe:
            httpGet:
              path: /admin/health
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /admin/health
              port: 8080
            initialDelaySeconds: 120
            periodSeconds: 30
      volumes:
        - name: config
          configMap:
            name: filodb-config
```

### Cassandra en Kubernetes

```yaml
# k8s/05-cassandra-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: cassandra
  namespace: filodb
spec:
  serviceName: cassandra
  replicas: 3
  selector:
    matchLabels:
      app: cassandra
  template:
    metadata:
      labels:
        app: cassandra
    spec:
      containers:
        - name: cassandra
          image: cassandra:3.11
          ports:
            - containerPort: 9042
              name: cql
            - containerPort: 7000
              name: intra-node
          env:
            - name: CASSANDRA_CLUSTER_NAME
              value: "filodb"
            - name: CASSANDRA_SEEDS
              value: "cassandra-0.cassandra.filodb.svc.cluster.local"
            - name: MAX_HEAP_SIZE
              value: "2G"
            - name: HEAP_NEWSIZE
              value: "512M"
          volumeMounts:
            - name: cassandra-data
              mountPath: /var/lib/cassandra
          resources:
            requests:
              memory: "4Gi"
              cpu: "2"
            limits:
              memory: "4Gi"
              cpu: "2"
  volumeClaimTemplates:
    - metadata:
        name: cassandra-data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: standard
        resources:
          requests:
            storage: 50Gi

---
apiVersion: v1
kind: Service
metadata:
  name: cassandra
  namespace: filodb
spec:
  clusterIP: None
  selector:
    app: cassandra
  ports:
    - port: 9042
      name: cql
```

### Despliegue Paso a Paso en Kubernetes

```bash
# 1. Crear namespace
kubectl apply -f k8s/00-namespace.yaml

# 2. Desplegar Cassandra
kubectl apply -f k8s/05-cassandra-statefulset.yaml

# 3. Esperar a que Cassandra esté listo
kubectl -n filodb wait --for=condition=ready pod/cassandra-0 --timeout=300s
kubectl -n filodb wait --for=condition=ready pod/cassandra-1 --timeout=300s
kubectl -n filodb wait --for=condition=ready pod/cassandra-2 --timeout=300s

# 4. Crear esquema de Cassandra
kubectl -n filodb exec cassandra-0 -- bash -c "
  cat > /tmp/ddl.cql << 'EOF'
  -- (contenido del DDL generado por schema-create.sh)
  EOF
  cqlsh -f /tmp/ddl.cql
"

# 5. Desplegar Kafka (usar operador Strimzi o similar)
# kubectl apply -f k8s/kafka/

# 6. Crear topic de Kafka
kubectl -n filodb exec kafka-0 -- kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 3 \
  --partitions 12 \
  --topic timeseries-prod

# 7. Desplegar ConfigMap y servicios de FiloDB
kubectl apply -f k8s/01-configmap.yaml
kubectl apply -f k8s/02-service-headless.yaml
kubectl apply -f k8s/03-service-lb.yaml

# 8. Desplegar FiloDB
kubectl apply -f k8s/04-statefulset.yaml

# 9. Verificar estado
kubectl -n filodb get pods
kubectl -n filodb logs filodb-server-0 -f

# 10. Verificar health
kubectl -n filodb port-forward svc/filodb-http 8080:8080
curl http://localhost:8080/admin/health
```

### Escalado en Kubernetes

```bash
# Escalar FiloDB a 5 nodos
kubectl -n filodb scale statefulset filodb-server --replicas=5

# Escalar Cassandra a 5 nodos
kubectl -n filodb scale statefulset cassandra --replicas=5

# Verificar redistribución de shards
curl http://localhost:8080/api/v1/cluster/prometheus/statusByAddress
```

---

## Configuración de Descubrimiento en Kubernetes

FiloDB soporta descubrimiento de nodos del cluster de forma nativa para Kubernetes:

### Opción 1: StatefulSet Hostname Format

```hocon
cluster-discovery {
  k8s-stateful-sets-hostname-format = "filodb-server-{}.filodb-headless.filodb.svc.cluster.local"
}
```

FiloDB extrae el ordinal del hostname del pod (e.g., `filodb-server-0`, `filodb-server-1`) y construye las direcciones de los peers.

### Opción 2: DNS SRV

```hocon
cluster-discovery {
  method = "dns-srv"
  dns-srv-name = "_akka._tcp.filodb-headless.filodb.svc.cluster.local"
}
```

### Opción 3: HTTP Seeds

```hocon
cluster-discovery {
  method = "http"
  seeds-base-url = "http://filodb-headless.filodb.svc.cluster.local:8080"
}
```

---

## Resumen de Puertos

| Puerto | Protocolo | Servicio |
|--------|-----------|----------|
| 8080 | HTTP | API REST (PromQL, admin, health) |
| 2552 | TCP | Akka Cluster (remoting) |
| 8888 | gRPC | Consultas distribuidas |
| 9042 | CQL | Cassandra |
| 9092 | TCP | Kafka |
| 2181 | TCP | ZooKeeper |
| 3000 | HTTP | Grafana |
