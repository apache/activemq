/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.prometheus;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Exposes ActiveMQ broker and destination JMX metrics via Prometheus
public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsServlet.class);

    private final PrometheusTextFormatWriter writer = PrometheusTextFormatWriter.create();

    // Metrics can be easily extended by adding them here with their base name.
    // Exporters take care of prefixes and suffixes.
    private static final MetricDefinition[] BROKER_METRICS = {
        new MetricDefinition("current_connections", "Current number of connections", "CurrentConnectionsCount", MetricType.GAUGE),
        new MetricDefinition("connections", "Total connections since last start", "TotalConnectionsCount", MetricType.COUNTER),
        new MetricDefinition("messages_enqueued", "Total messages enqueued since last start", "TotalEnqueueCount", MetricType.COUNTER),
        new MetricDefinition("messages_dequeued", "Total messages dequeued since last start", "TotalDequeueCount", MetricType.COUNTER),
        new MetricDefinition("consumers", "Current number of consumers", "TotalConsumerCount", MetricType.GAUGE),
        new MetricDefinition("producers", "Current number of producers", "TotalProducerCount", MetricType.GAUGE),
        new MetricDefinition("messages", "Current number of messages across all destinations", "TotalMessageCount", MetricType.GAUGE),
        new MetricDefinition("memory_percent_usage", "Percent (0-100) of memory limit used", "MemoryPercentUsage", MetricType.GAUGE),
        new MetricDefinition("memory_limit_bytes", "Memory limit in bytes", "MemoryLimit", MetricType.GAUGE),
        new MetricDefinition("store_percent_usage", "Percent (0-100) of store limit used", "StorePercentUsage", MetricType.GAUGE),
        new MetricDefinition("store_limit_bytes", "Store limit in bytes", "StoreLimit", MetricType.GAUGE),
        new MetricDefinition("temp_percent_usage", "Percent (0-100) of temp limit used", "TempPercentUsage", MetricType.GAUGE),
        new MetricDefinition("temp_limit_bytes", "Temp limit in bytes", "TempLimit", MetricType.GAUGE),
        new MetricDefinition("uptime_milliseconds", "Broker uptime in milliseconds", "UptimeMillis", MetricType.GAUGE),
        new MetricDefinition("queues", "Number of queues on the broker", "TotalQueuesCount", MetricType.GAUGE),
        new MetricDefinition("topics", "Number of topics on the broker", "TotalTopicsCount", MetricType.GAUGE),
        new MetricDefinition("job_scheduler_store_percent_usage", "Percent (0-100) of job scheduler store limit used", "JobSchedulerStorePercentUsage", MetricType.GAUGE),
        new MetricDefinition("job_scheduler_store_limit_bytes", "Job scheduler store limit in bytes", "JobSchedulerStoreLimit", MetricType.GAUGE)
    };

    private static final String[] DESTINATION_TYPES = {"Queue", "Topic", "TempQueue", "TempTopic"};
    private static final MetricDefinition[] DESTINATION_METRICS = {
        new MetricDefinition("messages", "Number of messages in this destination", "QueueSize", MetricType.GAUGE),
        new MetricDefinition("enqueued", "Total messages enqueued to this destination since last start", "EnqueueCount", MetricType.COUNTER),
        new MetricDefinition("dequeued", "Total messages dequeued from destination since last start", "DequeueCount", MetricType.COUNTER),
        new MetricDefinition("dispatched", "Total messages dispatched from destination since last start", "DispatchCount", MetricType.COUNTER),
        new MetricDefinition("messages_inflight", "Messages dispatched but not acknowledged", "InFlightCount", MetricType.GAUGE),
        new MetricDefinition("expired", "Total messages expired since last start", "ExpiredCount", MetricType.COUNTER),
        new MetricDefinition("consumers", "Number of consumers", "ConsumerCount", MetricType.GAUGE),
        new MetricDefinition("producers", "Number of producers", "ProducerCount", MetricType.GAUGE),
        new MetricDefinition("memory_percent_usage", "Percent (0-100) of destination memory limit used", "MemoryPercentUsage", MetricType.GAUGE),
        new MetricDefinition("memory_limit_bytes", "Memory limit for this destination in bytes", "MemoryLimit", MetricType.GAUGE),
        new MetricDefinition("memory_usage_bytes", "Memory used by this destination in bytes", "MemoryUsageByteCount", MetricType.GAUGE),
        new MetricDefinition("store_message_size_bytes", "Store message size in bytes", "StoreMessageSize", MetricType.GAUGE),
        new MetricDefinition("average_enqueue_time_milliseconds", "Average time (since last start) messages waited before dispatch", "AverageEnqueueTime", MetricType.GAUGE)
    };

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        final boolean perObject = request != null && "true".equalsIgnoreCase(request.getParameter("per_object"));
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // If the mBeanServer is unavailable nothing useful can be produced, so return a 500
        final Set<ObjectName> brokers;
        final Map<String, Set<ObjectName>> destinationsByType = new LinkedHashMap<>();
        try {
            brokers = mBeanServer.queryNames(new ObjectName("org.apache.activemq:type=Broker,brokerName=*"), null);
            if (perObject) {
                // Scraping on brokers with many destinations can be expensive.
                for (final String type : DESTINATION_TYPES) {
                    destinationsByType.put(type, mBeanServer.queryNames(new ObjectName(
                            "org.apache.activemq:type=Broker,brokerName=*,destinationType=" + type
                                    + ",destinationName=*"), null));
                }
            }
        } catch (final Exception exception) {
            LOG.warn("Prometheus scrape failed while querying broker MBeans", exception);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Metrics collection failed");
            return;
        }

        // Failed items are skipped or default to 0 (when not possible) to allow partial data..
        final MetricSnapshots.Builder snapshots = MetricSnapshots.builder();
        collectBrokerMetrics(mBeanServer, snapshots, brokers);
        for (final Map.Entry<String, Set<ObjectName>> entry : destinationsByType.entrySet()) {
            collectDestinationMetrics(mBeanServer, snapshots, entry.getKey(), entry.getValue());
        }

        response.setContentType(writer.getContentType());
        response.setStatus(HttpServletResponse.SC_OK);
        final OutputStream out = response.getOutputStream();
        writer.write(out, snapshots.build());
        out.flush();
    }

    private void collectBrokerMetrics(final MBeanServer mBeanServer, final MetricSnapshots.Builder snapshots,
            final Set<ObjectName> brokers) {
        // In case there is a network of brokers, only use the local one.
        // Skip unreadable names.
        final Map<ObjectName, Labels> identified = new LinkedHashMap<>();
        for (final ObjectName broker : brokers) {
            final String brokerName = resolveStringAttribute(mBeanServer, broker, "BrokerName");
            if (brokerName != null) {
                identified.put(broker, Labels.of("broker", brokerName));
            }
        }
        if (identified.isEmpty()) {
            return;
        }

        for (final MetricDefinition metric : BROKER_METRICS) {
            final String metricName = "activemq_broker_" + metric.name;
            final MetricSnapshot snapshot = buildSnapshot(mBeanServer, metricName, metric, metric.help, identified);
            if (snapshot != null) {
                snapshots.metricSnapshot(snapshot);
            }
        }
    }

    private void collectDestinationMetrics(final MBeanServer mBeanServer, final MetricSnapshots.Builder snapshots,
            final String type, final Set<ObjectName> destinations) {
        if (destinations.isEmpty()) {
            return;
        }
        final String typeLower = type.toLowerCase();

        final Map<ObjectName, Labels> labelled = new LinkedHashMap<>();
        for (final ObjectName destination : destinations) {
            final String brokerName = destination.getKeyProperty("brokerName");
            final String destinationName = destination.getKeyProperty("destinationName");
            labelled.put(destination, Labels.of(
                    "broker", brokerName == null ? "unknown" : brokerName,
                    "destination", destinationName == null ? "unknown" : destinationName));
        }

        for (final MetricDefinition metric : DESTINATION_METRICS) {
            final String metricName = "activemq_" + typeLower + "_" + metric.name;
            final MetricSnapshot snapshot = buildSnapshot(mBeanServer, metricName, metric,
                    String.format(metric.help, typeLower), labelled);
            if (snapshot != null) {
                snapshots.metricSnapshot(snapshot);
            }
        }
    }

    // Builds one metric family (a gauge or counter) with a data point per object.
    private MetricSnapshot buildSnapshot(final MBeanServer mBeanServer, final String metricName,
            final MetricDefinition metric, final String help, final Map<ObjectName, Labels> objects) {
        if (metric.type == MetricType.COUNTER) {
            final CounterSnapshot.Builder builder = CounterSnapshot.builder()
                    .name(metricName)
                    .help(help);
            for (final Map.Entry<ObjectName, Labels> entry : objects.entrySet()) {
                builder.dataPoint(CounterDataPointSnapshot.builder()
                        .labels(entry.getValue())
                        .value(getNumber(mBeanServer, entry.getKey(), metric.attribute))
                        .build());
            }
            return builder.build();
        }

        final GaugeSnapshot.Builder builder = GaugeSnapshot.builder()
                .name(metricName)
                .help(help);
        for (final Map.Entry<ObjectName, Labels> entry : objects.entrySet()) {
            builder.dataPoint(GaugeDataPointSnapshot.builder()
                    .labels(entry.getValue())
                    .value(getNumber(mBeanServer, entry.getKey(), metric.attribute))
                    .build());
        }
        return builder.build();
    }

    private String resolveStringAttribute(final MBeanServer mBeanServer, final ObjectName name, final String attribute) {
        // Partial results are better than no results if something goes wrong
        try {
            final Object value = mBeanServer.getAttribute(name, attribute);
            if (value instanceof String) {
                return (String) value;
            }
        } catch (final Exception exception) {
            LOG.debug("Skipping object {}: identity attribute {} unavailable", name, attribute, exception);
        }
        return null;
    }

    private double getNumber(final MBeanServer mBeanServer, final ObjectName name, final String attribute) {
        try {
            final Object value = mBeanServer.getAttribute(name, attribute);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
        } catch (final Exception exception) {
            // Some attributes are not available on every ActiveMQ deployment (eg: bridge metrics).
            LOG.debug("Reporting 0 for {} on {}: attribute unavailable", attribute, name, exception);
        }
        return 0;
    }

    private static final class MetricDefinition {
        private final String name;
        private final String help;
        private final String attribute;
        private final MetricType type;

        private MetricDefinition(final String name, final String help, final String attribute, final MetricType type) {
            this.name = name;
            this.help = help;
            this.attribute = attribute;
            this.type = type;
        }
    }

    enum MetricType {
        GAUGE,
        COUNTER
    }
}
