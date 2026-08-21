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
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsServlet.class);
    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    // Metrics can be easily extended by adding them here
    private static final MetricDefinition[] BROKER_METRICS = {
        new MetricDefinition("connections", "Current number of connections", "CurrentConnectionsCount", MetricType.GAUGE),
        new MetricDefinition("connections_total", "Total connections since last start", "TotalConnectionsCount", MetricType.COUNTER),
        new MetricDefinition("messages_enqueued_total", "Total messages enqueued since last start", "TotalEnqueueCount", MetricType.COUNTER),
        new MetricDefinition("messages_dequeued_total", "Total messages dequeued since last start", "TotalDequeueCount", MetricType.COUNTER),
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

    private static final MetricDefinition[] DESTINATION_METRICS = {
        new MetricDefinition("messages", "Number of messages in this destination", "QueueSize", MetricType.GAUGE),
        new MetricDefinition("enqueued_total", "Total messages enqueued to this destination since last start", "EnqueueCount", MetricType.COUNTER),
        new MetricDefinition("dequeued_total", "Total messages dequeued from destination since last start", "DequeueCount", MetricType.COUNTER),
        new MetricDefinition("dispatched_total", "Total messages dispatched from destination since last start", "DispatchCount", MetricType.COUNTER),
        new MetricDefinition("message_inflight_count", "Messages dispatched but not acknowledged", "InFlightCount", MetricType.GAUGE),
        new MetricDefinition("expired_total", "Total messages expired since last start", "ExpiredCount", MetricType.COUNTER),
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
        final Set<ObjectName> queues;
        final Set<ObjectName> topics;
        try {
            brokers = mBeanServer.queryNames(new ObjectName("org.apache.activemq:type=Broker,brokerName=*"), null);
            if (perObject) {
                // Scraping destinations on brokers with many queues or topics can be expensive.
                queues = mBeanServer.queryNames(new ObjectName(
                        "org.apache.activemq:type=Broker,brokerName=*,destinationType=Queue,destinationName=*"), null);
                topics = mBeanServer.queryNames(new ObjectName(
                        "org.apache.activemq:type=Broker,brokerName=*,destinationType=Topic,destinationName=*"), null);
            } else {
                queues = Collections.emptySet();
                topics = Collections.emptySet();
            }
        } catch (final Exception exception) {
            LOG.warn("Prometheus scrape failed while querying broker MBeans", exception);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Metrics collection failed");
            return;
        }

        // Stream metrics to avoid keeping the response body in memory
        // Failed items are skipped or default to 0 (when not possible) to allow the scrape to continue
        response.setContentType(CONTENT_TYPE);
        response.setStatus(HttpServletResponse.SC_OK);
        final PrintWriter writer = response.getWriter();
        writeBrokerMetrics(mBeanServer, writer, brokers);
        if (perObject) {
            writeDestinationMetrics(mBeanServer, writer, "Queue", queues);
            writeDestinationMetrics(mBeanServer, writer, "Topic", topics);
        }
        writer.flush();
    }

    private void writeBrokerMetrics(final MBeanServer mBeanServer, final PrintWriter writer, final Set<ObjectName> brokers) {
        // In case there is a network of brokers, only use the local one
        // Unreadable names are skipped
        final Map<ObjectName, String> identified = new LinkedHashMap<>();
        for (final ObjectName broker : brokers) {
            final String brokerName = resolveStringAttribute(mBeanServer, broker, "BrokerName");
            if (brokerName != null) {
                identified.put(broker, "broker=\"" + sanitizeLabel(brokerName) + "\"");
            }
        }

        for (final MetricDefinition metric : BROKER_METRICS) {
            final String metricName = "activemq_broker_" + metric.name;
            writeMetadata(writer, metricName, metric);
            for (final Map.Entry<ObjectName, String> entry : identified.entrySet()) {
                writeSample(writer, metricName, entry.getValue(), getNumber(mBeanServer, entry.getKey(), metric.attribute));
            }
        }
    }

    private void writeDestinationMetrics(final MBeanServer mBeanServer, final PrintWriter writer, final String type,
            final Set<ObjectName> destinations) {
        final String typeLower = type.toLowerCase();

        for (final MetricDefinition metric : DESTINATION_METRICS) {
            final String metricName = "activemq_" + typeLower + "_" + metric.name;
            writeMetadata(writer, metricName, metric.withFormattedHelp(typeLower));
            for (final ObjectName destination : destinations) {
                final String brokerName = sanitizeLabel(destination.getKeyProperty("brokerName"));
                final String destinationName = sanitizeLabel(destination.getKeyProperty("destinationName"));
                final String labels = String.format("broker=\"%s\",destination=\"%s\"", brokerName, destinationName);
                writeSample(writer, metricName, labels, getNumber(mBeanServer, destination, metric.attribute));
            }
        }
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

    private void writeMetadata(final PrintWriter writer, final String metric, final MetricDefinition def) {
        writer.println("# HELP " + metric + " " + def.help);
        writer.println("# TYPE " + metric + " " + def.type.prometheusName());
    }

    private void writeSample(final PrintWriter writer, final String metric, final String labels, final double value) {
        final String rendered;
        if (Double.isNaN(value)) {
            rendered = "NaN";
        } else if (value == Double.POSITIVE_INFINITY) {
            rendered = "+Inf";
        } else if (value == Double.NEGATIVE_INFINITY) {
            rendered = "-Inf";
        } else if (value == (long) value) {
            rendered = Long.toString((long) value);
        } else {
            rendered = Double.toString(value);
        }
        writer.println(metric + "{" + labels + "} " + rendered);
    }

    static String sanitizeLabel(final String value) {
        if (value == null) {
            return "unknown";
        }
        // See: https://prometheus.io/docs/instrumenting/exposition_formats/
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
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

        private MetricDefinition withFormattedHelp(final String arg) {
            return new MetricDefinition(name, String.format(help, arg), attribute, type);
        }
    }

    enum MetricType {
        GAUGE("gauge"),
        COUNTER("counter");

        private final String prometheusName;

        MetricType(final String prometheusName) {
            this.prometheusName = prometheusName;
        }

        String prometheusName() {
            return prometheusName;
        }
    }
}
