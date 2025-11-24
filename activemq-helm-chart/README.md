# ActiveMQ Helm Chart

A Helm chart for deploying Apache ActiveMQ on Kubernetes.

## Introduction

This chart bootstraps an ActiveMQ deployment on a Kubernetes cluster using the Helm package manager.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- PV provisioner support in the underlying infrastructure (if persistence is enabled)

## Installing the Chart

To install the chart with the release name `my-activemq`:

```bash
helm install my-activemq .
```

## Uninstalling the Chart

To uninstall/delete the `my-activemq` deployment:

```bash
helm uninstall my-activemq
```

## Configuration

The following table lists the configurable parameters of the ActiveMQ chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | ActiveMQ image repository | `apache/activemq-classic` |
| `image.tag` | ActiveMQ image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `auth.username` | ActiveMQ admin username | `admin` |
| `auth.password` | ActiveMQ admin password | `changeme` |
| `service.type` | Kubernetes service type | `ClusterIP` |
| `service.port` | OpenWire service port | `61616` |
| `service.webConsolePort` | Web Console port | `8161` |
| `persistence.enabled` | Enable persistence using PVC | `true` |
| `persistence.storageClass` | PVC Storage Class | `""` |
| `persistence.accessMode` | PVC Access Mode | `ReadWriteOnce` |
| `persistence.size` | PVC Storage Request | `8Gi` |
| `resources.limits.cpu` | CPU limit | `1000m` |
| `resources.limits.memory` | Memory limit | `2Gi` |
| `resources.requests.cpu` | CPU request | `500m` |
| `resources.requests.memory` | Memory request | `1Gi` |
| `livenessProbe.enabled` | Enable liveness probe | `true` |
| `readinessProbe.enabled` | Enable readiness probe | `true` |
| `ingress.enabled` | Enable ingress | `false` |
| `config.activemqXmlPath` | Path to custom activemq.xml file | `""` |

## Accessing ActiveMQ

### Web Console

The ActiveMQ Web Console is accessible on port 8161. To access it locally:

```bash
kubectl port-forward svc/my-activemq 8161:8161
```

Then open http://localhost:8161 in your browser and login with the configured credentials.

### OpenWire Connection

Applications can connect to ActiveMQ on port 61616 using the service name:

```
tcp://my-activemq:61616
```

## Custom Configuration

To use a custom `activemq.xml` configuration file, place your file in the chart directory and specify the path:

```bash
cp activemq_sample.xml mq-activemq.xml
```

```yaml
config:
  activemqXmlPath: "my-activemq.xml"
```

## Persistence

The ActiveMQ image stores data at the `/opt/apache-activemq/data` path of the container.

By default, the chart mounts a Persistent Volume at this location. The volume is created using dynamic volume provisioning.