<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# ActiveMQ Helm Chart

A Helm chart for deploying [Apache ActiveMQ](https://activemq.apache.org/) on Kubernetes.

## Introduction

This chart bootstraps an Apache ActiveMQ broker deployment on a Kubernetes cluster using the [Helm](https://helm.sh) package manager. It deploys ActiveMQ as a StatefulSet with persistent storage for the KahaDB message store.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.10+
- PV provisioner support in the underlying infrastructure (if persistence is enabled)

## Installing the Chart

To install the chart with the release name `my-activemq`:

```bash
helm install my-activemq ./helm/activemq
```

To install with custom values:

```bash
helm install my-activemq ./helm/activemq -f my-values.yaml
```

## Uninstalling the Chart

To uninstall/delete the `my-activemq` deployment:

```bash
helm uninstall my-activemq
```

> **Note:** Uninstalling the chart does not delete PersistentVolumeClaims created by the StatefulSet. To delete them:
> ```bash
> kubectl delete pvc -l app.kubernetes.io/instance=my-activemq
> ```

## Configuration

### General

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of broker replicas | `1` |
| `image.repository` | Container image repository | `apache/activemq-classic` |
| `image.tag` | Image tag (defaults to chart `appVersion`) | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `imagePullSecrets` | Image pull secrets for private registries | `[]` |
| `nameOverride` | Override the chart name | `""` |
| `fullnameOverride` | Override the full release name | `""` |
| `terminationGracePeriodSeconds` | Graceful shutdown timeout | `60` |

### Broker

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.credentials.username` | Broker connection username | `admin` |
| `broker.credentials.password` | Broker connection password | `admin` |
| `broker.credentials.existingSecret` | Use an existing secret for connection credentials | `""` |
| `broker.web.username` | Web console username | `admin` |
| `broker.web.password` | Web console password | `admin` |
| `broker.web.existingSecret` | Use an existing secret for web credentials | `""` |
| `broker.jmx.enabled` | Enable JMX | `false` |
| `broker.jmx.username` | JMX username | `admin` |
| `broker.jmx.password` | JMX password | `activemq` |
| `broker.jmx.existingSecret` | Use an existing secret for JMX credentials | `""` |
| `broker.jvmMemory` | JVM memory options | `"-Xms64M -Xmx1G"` |
| `broker.jvmOpts` | Additional JVM options | `""` |
| `broker.activemqXml` | Custom `activemq.xml` content | `""` |
| `broker.jettyXml` | Custom `jetty.xml` content | `""` |
| `broker.log4j2Properties` | Custom `log4j2.properties` content | `""` |

### Service

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Broker service type | `ClusterIP` |
| `service.annotations` | Broker service annotations | `{}` |
| `service.openwire.enabled` | Enable OpenWire transport | `true` |
| `service.openwire.port` | OpenWire service port | `61616` |
| `service.amqp.enabled` | Enable AMQP transport | `true` |
| `service.amqp.port` | AMQP service port | `5672` |
| `service.stomp.enabled` | Enable STOMP transport | `true` |
| `service.stomp.port` | STOMP service port | `61613` |
| `service.mqtt.enabled` | Enable MQTT transport | `true` |
| `service.mqtt.port` | MQTT service port | `1883` |
| `service.ws.enabled` | Enable WebSocket transport | `true` |
| `service.ws.port` | WebSocket service port | `61614` |

### Web Console

| Parameter | Description | Default |
|-----------|-------------|---------|
| `webConsole.type` | Web console service type | `ClusterIP` |
| `webConsole.port` | Web console HTTP port | `8161` |
| `webConsole.annotations` | Web console service annotations | `{}` |

### Ingress

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable ingress for the web console | `false` |
| `ingress.className` | Ingress class name | `""` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | Ingress hosts configuration | See `values.yaml` |
| `ingress.tls` | Ingress TLS configuration | `[]` |

### Persistence

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistent storage for KahaDB | `true` |
| `persistence.storageClassName` | PVC storage class | `""` |
| `persistence.accessModes` | PVC access modes | `["ReadWriteOnce"]` |
| `persistence.size` | PVC storage size | `8Gi` |
| `persistence.annotations` | PVC annotations | `{}` |
| `persistence.selector` | PVC selector for matching PVs | `{}` |

### Resources and Probes

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.requests.cpu` | CPU request | `500m` |
| `resources.requests.memory` | Memory request | `1Gi` |
| `resources.limits.cpu` | CPU limit | `2` |
| `resources.limits.memory` | Memory limit | `2Gi` |
| `startupProbe` | Startup probe (TCP on OpenWire port) | See `values.yaml` |
| `livenessProbe` | Liveness probe (HTTP on web console) | See `values.yaml` |
| `readinessProbe` | Readiness probe (TCP on OpenWire port) | See `values.yaml` |

### Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |
| `podSecurityContext.runAsNonRoot` | Run as non-root | `true` |
| `podSecurityContext.runAsUser` | Run as UID | `1000` |
| `podSecurityContext.runAsGroup` | Run as GID | `1000` |
| `podSecurityContext.fsGroup` | Filesystem group | `1000` |
| `securityContext.allowPrivilegeEscalation` | Allow privilege escalation | `false` |
| `securityContext.capabilities.drop` | Dropped capabilities | `["ALL"]` |

### Availability

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podDisruptionBudget.enabled` | Enable Pod Disruption Budget | `false` |
| `podDisruptionBudget.minAvailable` | Minimum available pods | `1` |

### Monitoring

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |

### Scheduling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |
| `topologySpreadConstraints` | Topology spread constraints | `[]` |
| `podAnnotations` | Pod annotations | `{}` |
| `podLabels` | Pod labels | `{}` |

### Extensibility

| Parameter | Description | Default |
|-----------|-------------|---------|
| `extraEnv` | Extra environment variables | `[]` |
| `extraVolumes` | Extra volumes | `[]` |
| `extraVolumeMounts` | Extra volume mounts | `[]` |
| `initContainers` | Init containers | `[]` |

## Accessing ActiveMQ

### Web Console

The ActiveMQ Web Console is available on port 8161. To access it locally:

```bash
kubectl port-forward svc/my-activemq-web 8161:8161
```

Then open http://localhost:8161 in your browser and log in with the configured credentials (default: `admin`/`admin`).

### Broker Connections

Applications can connect to ActiveMQ using the following protocols from within the cluster:

| Protocol | URL |
|----------|-----|
| OpenWire (JMS) | `tcp://my-activemq:61616` |
| AMQP | `amqp://my-activemq:5672` |
| STOMP | `stomp://my-activemq:61613` |
| MQTT | `mqtt://my-activemq:1883` |
| WebSocket | `ws://my-activemq:61614` |

To connect from outside the cluster, use port-forwarding:

```bash
kubectl port-forward svc/my-activemq 61616:61616
```

Then connect to `tcp://localhost:61616`.

## Custom Configuration

You can provide a custom `activemq.xml` directly in your values file:

```yaml
broker:
  activemqXml: |
    <beans xmlns="http://www.springframework.org/schema/beans"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd
        http://activemq.apache.org/schema/core
        http://activemq.apache.org/schema/core/activemq-core.xsd">
      <broker xmlns="http://activemq.apache.org/schema/core"
              brokerName="localhost" dataDirectory="${activemq.data}">
        <!-- your custom configuration here -->
      </broker>
      <import resource="jetty.xml"/>
    </beans>
```

Similarly, you can customize `jetty.xml` and `log4j2.properties` via `broker.jettyXml` and `broker.log4j2Properties`.

See the [examples/](examples/) directory for complete example values files:
- `activemq-custom.yaml` - Custom broker configuration with `activemq.xml`
- `production.yaml` - Production-ready settings with external secrets, ingress, and anti-affinity

## Credentials

Credentials are stored in Kubernetes Secrets. For production, use existing secrets instead of plaintext values:

```yaml
broker:
  credentials:
    existingSecret: "my-activemq-credentials"
  web:
    existingSecret: "my-activemq-web-credentials"
```

Create the secrets beforehand:

```bash
kubectl create secret generic my-activemq-credentials \
  --from-literal=connection-username=admin \
  --from-literal=connection-password=s3cur3p4ss

kubectl create secret generic my-activemq-web-credentials \
  --from-literal=web-username=admin \
  --from-literal=web-password=s3cur3p4ss
```

## Persistence

The ActiveMQ image stores message data (KahaDB) at `/opt/apache-activemq/data`.

By default, the chart creates a PersistentVolumeClaim for each pod via the StatefulSet's `volumeClaimTemplates`. The volume is provisioned dynamically using the cluster's default StorageClass.

To disable persistence (data will be lost on pod restart):

```yaml
persistence:
  enabled: false
```

## Monitoring

To enable Prometheus monitoring via the ServiceMonitor CRD:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
```

This requires the [Prometheus Operator](https://prometheus-operator.dev/) to be installed in your cluster.
