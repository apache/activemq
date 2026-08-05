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
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.Set;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
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
        new MetricDefinition("uptime_milliseconds", "Broker uptime in milliseconds", "UptimeMillis", MetricType.GAUGE)
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
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        boolean perObject = request != null && "true".equalsIgnoreCase(request.getParameter("per_object"));

        StringWriter output = new StringWriter();
        PrintWriter writer = new PrintWriter(output);

        try {
            writeMetrics(ManagementFactory.getPlatformMBeanServer(), writer, perObject);
        } catch (Exception exception) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Metrics collection failed");
            return;
        }

        response.setContentType(CONTENT_TYPE);
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write(output.toString());
    }

    void writeMetrics(MBeanServer mBeanServer, PrintWriter writer, boolean perObject) throws Exception {
        writeBrokerMetrics(mBeanServer, writer);
        // Scraping this by default on brokers with lots of queues or topics might be expensive
        if (perObject) {
            writeDestinationMetrics(mBeanServer, writer, "Queue");
            writeDestinationMetrics(mBeanServer, writer, "Topic");
        }
        writer.flush();
    }

    private void writeBrokerMetrics(MBeanServer mBeanServer, PrintWriter writer) throws Exception {
        ObjectName pattern = new ObjectName("org.apache.activemq:type=Broker,brokerName=*");
        Set<ObjectName> brokers = mBeanServer.queryNames(pattern, null);

        for (MetricDefinition metric : BROKER_METRICS) {
            String metricName = "activemq_broker_" + metric.name;
            writeMetadata(writer, metricName, metric);
            for (ObjectName broker : brokers) {
                String brokerName = sanitizeLabel((String) mBeanServer.getAttribute(broker, "BrokerName"));
                String labels = "broker=\"" + brokerName + "\"";
                writeSample(writer, metricName, labels, getNumber(mBeanServer, broker, metric.attribute));
            }
        }
    }

    private void writeDestinationMetrics(MBeanServer mBeanServer, PrintWriter writer, String type) throws Exception {
        String queryPattern = "org.apache.activemq:type=Broker,brokerName=*,destinationType=" + type + ",destinationName=*";
        Set<ObjectName> destinations = mBeanServer.queryNames(new ObjectName(queryPattern), null);
        String typeLower = type.toLowerCase();

        for (MetricDefinition metric : DESTINATION_METRICS) {
            String metricName = "activemq_" + typeLower + "_" + metric.name;
            writeMetadata(writer, metricName, metric.withFormattedHelp(typeLower));
            for (ObjectName destination : destinations) {
                String brokerName = sanitizeLabel(destination.getKeyProperty("brokerName"));
                String destinationName = sanitizeLabel(destination.getKeyProperty("destinationName"));
                String labels = String.format("broker=\"%s\",destination=\"%s\"", brokerName, destinationName);
                writeSample(writer, metricName, labels, getNumber(mBeanServer, destination, metric.attribute));
            }
        }
    }

    private double getNumber(MBeanServer mBeanServer, ObjectName name, String attribute) {
        try {
            Object value = mBeanServer.getAttribute(name, attribute);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
        } catch (Exception ignored) {
            // Some attributes are not available on every ActiveMQ deployment (eg: bridge metrics)
        }
        return 0;
    }

    private void writeMetadata(PrintWriter writer, String metric, MetricDefinition def) {
        writer.println("# HELP " + metric + " " + def.help);
        writer.println("# TYPE " + metric + " " + def.type.prometheusName());
    }

    private void writeSample(PrintWriter writer, String metric, String labels, double value) {
        if (value == (long) value) {
            writer.println(metric + "{" + labels + "} " + (long) value);
        } else {
            writer.println(metric + "{" + labels + "} " + value);
        }
    }

    static String sanitizeLabel(String value) {
        if (value == null) {
            return "unknown";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static final class MetricDefinition {
        private final String name;
        private final String help;
        private final String attribute;
        private final MetricType type;

        private MetricDefinition(String name, String help, String attribute, MetricType type) {
            this.name = name;
            this.help = help;
            this.attribute = attribute;
            this.type = type;
        }

        private MetricDefinition withFormattedHelp(String arg) {
            return new MetricDefinition(name, String.format(help, arg), attribute, type);
        }
    }

    enum MetricType {
        GAUGE("gauge"),
        COUNTER("counter");

        private final String prometheusName;

        MetricType(String prometheusName) {
            this.prometheusName = prometheusName;
        }

        String prometheusName() {
            return prometheusName;
        }
    }
}
