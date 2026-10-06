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

import static org.apache.activemq.prometheus.PrometheusConstants.BROKER_METRIC_PREFIX;
import static org.apache.activemq.prometheus.PrometheusConstants.DEFAULT_DESTINATION_TYPES;
import static org.apache.activemq.prometheus.PrometheusConstants.DESTINATION_METRIC_PREFIX;
import static org.apache.activemq.prometheus.PrometheusConstants.INIT_PARAM_DESTINATION_TYPES;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_BROKER;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_DESTINATION;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_DESTINATION_TYPE;
import static org.apache.activemq.prometheus.PrometheusConstants.PARAM_PER_OBJECT;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.JMX;
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

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes ActiveMQ broker and destination JMX metrics in the Prometheus text format.
 * <p>
 * {@code GET /metrics} returns broker level metrics.
 * {@code GET /metrics?per_object=true} also returns per destination metrics.
 * {@link PrometheusConstants#INIT_PARAM_DESTINATION_TYPES} determines which destinations are added.
 * <p>
 * Brokers are found in the {@link BrokerRegistry}. Each broker's destinations come from its
 * {@link BrokerViewMBean}, and values are read through {@link BrokerViewMBean} and
 * {@link DestinationViewMBean} proxies.
 */
public class PrometheusMetricsServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsServlet.class);

    private final PrometheusTextFormatWriter writer = PrometheusTextFormatWriter.create();
    private final transient Supplier<List<JmxBroker>> brokers;
    private Set<DestinationType> enabledDestinationTypes = parseDestinationTypes(null);

    /** A broker to scrape: its MBean name and the MBean server it is registered in. */
    static final class JmxBroker {
        final ObjectName name;
        final MBeanServer server;

        JmxBroker(final ObjectName name, final MBeanServer server) {
            this.name = name;
            this.server = server;
        }
    }

    /**
     * Destination types that {@code per_object=true} can include.
     * Each destination of an enabled type gets its own series.
     * Each destination type has:
     * <ul>
     *   <li>{@code label}, for example {@code temp-queue}: the name used in the {@code destinationTypes}
     *       setting and in the {@code destination_type} label. It is the broker's destination URI scheme
     *       ({@code temp-queue://}).</li>
     *   <li>{@code list}: the {@link BrokerViewMBean} getter that returns the broker's destinations of
     *       this type.</li>
     * </ul>
     */
    enum DestinationType {
        QUEUE("queue", BrokerViewMBean::getQueues),
        TOPIC("topic", BrokerViewMBean::getTopics),
        TEMP_QUEUE("temp-queue", BrokerViewMBean::getTemporaryQueues),
        TEMP_TOPIC("temp-topic", BrokerViewMBean::getTemporaryTopics);

        final String label;
        final Function<BrokerViewMBean, ObjectName[]> list;

        DestinationType(final String label, final Function<BrokerViewMBean, ObjectName[]> list) {
            this.label = label;
            this.list = list;
        }

        static DestinationType fromLabel(final String label) {
            for (final DestinationType type : values()) {
                if (type.label.equals(label)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown destination type '" + label + "', expected one of "
                    + Arrays.stream(values()).map(type -> type.label).collect(Collectors.joining(", ")));
        }
    }

    // Metrics can be extended by adding them here
    private static final List<MetricDefinition<BrokerViewMBean>> BROKER_METRICS = List.of(
        gauge("current_connections", "Current number of connections", BrokerViewMBean::getCurrentConnectionsCount),
        counter("connections", "Total connections since last start", BrokerViewMBean::getTotalConnectionsCount),
        counter("messages_enqueued", "Total messages enqueued since last start", BrokerViewMBean::getTotalEnqueueCount),
        counter("messages_dequeued", "Total messages dequeued since last start", BrokerViewMBean::getTotalDequeueCount),
        gauge("consumers", "Current number of consumers", BrokerViewMBean::getTotalConsumerCount),
        gauge("producers", "Current number of producers", BrokerViewMBean::getTotalProducerCount),
        gauge("messages", "Current number of messages across all destinations", BrokerViewMBean::getTotalMessageCount),
        gauge("memory_percent_usage", "Percent (0-100) of memory limit used", BrokerViewMBean::getMemoryPercentUsage),
        gauge("memory_limit_bytes", "Memory limit in bytes", BrokerViewMBean::getMemoryLimit),
        gauge("store_percent_usage", "Percent (0-100) of store limit used", BrokerViewMBean::getStorePercentUsage),
        gauge("store_limit_bytes", "Store limit in bytes", BrokerViewMBean::getStoreLimit),
        gauge("temp_percent_usage", "Percent (0-100) of temp limit used", BrokerViewMBean::getTempPercentUsage),
        gauge("temp_limit_bytes", "Temp limit in bytes", BrokerViewMBean::getTempLimit),
        gauge("uptime_milliseconds", "Broker uptime in milliseconds", BrokerViewMBean::getUptimeMillis),
        gauge("queues", "Number of queues on the broker", BrokerViewMBean::getTotalQueuesCount),
        gauge("topics", "Number of topics on the broker", BrokerViewMBean::getTotalTopicsCount),
        gauge("job_scheduler_store_percent_usage", "Percent (0-100) of job scheduler store limit used", BrokerViewMBean::getJobSchedulerStorePercentUsage),
        gauge("job_scheduler_store_limit_bytes", "Job scheduler store limit in bytes", BrokerViewMBean::getJobSchedulerStoreLimit)
    );

    private static final List<MetricDefinition<DestinationViewMBean>> DESTINATION_METRICS = List.of(
        gauge("messages", "Number of messages in this destination", DestinationViewMBean::getQueueSize),
        counter("enqueued", "Total messages enqueued to this destination since last start", DestinationViewMBean::getEnqueueCount),
        counter("dequeued", "Total messages dequeued from destination since last start", DestinationViewMBean::getDequeueCount),
        counter("dispatched", "Total messages dispatched from destination since last start", DestinationViewMBean::getDispatchCount),
        gauge("messages_inflight", "Messages dispatched but not acknowledged", DestinationViewMBean::getInFlightCount),
        counter("expired", "Total messages expired since last start", DestinationViewMBean::getExpiredCount),
        gauge("consumers", "Number of consumers", DestinationViewMBean::getConsumerCount),
        gauge("producers", "Number of producers", DestinationViewMBean::getProducerCount),
        gauge("memory_percent_usage", "Percent (0-100) of destination memory limit used", DestinationViewMBean::getMemoryPercentUsage),
        gauge("memory_limit_bytes", "Memory limit for this destination in bytes", DestinationViewMBean::getMemoryLimit),
        gauge("memory_usage_bytes", "Memory used by this destination in bytes", DestinationViewMBean::getMemoryUsageByteCount),
        gauge("store_message_size_bytes", "Store message size in bytes", DestinationViewMBean::getStoreMessageSize),
        gauge("average_enqueue_time_milliseconds", "Average time (since last start) messages waited before dispatch", DestinationViewMBean::getAverageEnqueueTime)
    );

    /** Scrapes every JMX-enabled broker registered in this JVM's {@link BrokerRegistry}. */
    public PrometheusMetricsServlet() {
        this(PrometheusMetricsServlet::registeredBrokers);
    }

    /** Scrapes the given brokers. Package-private so tests can supply MBeans without a running broker. */
    PrometheusMetricsServlet(final Supplier<List<JmxBroker>> brokers) {
        this.brokers = brokers;
    }

    @Override
    public void init() throws ServletException {
        String destinationTypesParam = getInitParameter(INIT_PARAM_DESTINATION_TYPES);
        if (destinationTypesParam == null) {
            destinationTypesParam = getServletContext().getInitParameter(INIT_PARAM_DESTINATION_TYPES);
        }
        try {
            enabledDestinationTypes = parseDestinationTypes(destinationTypesParam);
        } catch (final IllegalArgumentException exception) {
            throw new ServletException("Invalid " + INIT_PARAM_DESTINATION_TYPES + ": " + exception.getMessage(), exception);
        }
        LOG.info("Prometheus metrics servlet reporting destination types {} when {}=true",
                enabledDestinationTypes.stream().map(type -> type.label).collect(Collectors.toList()), PARAM_PER_OBJECT);
    }

    /**
     * Parses the {@code destinationTypes} init parameter, for example {@code "queue, temp-queue"}.
     * {@code null} (the parameter is not set) selects the default, {@code "queue,topic"}.
     * A value with no labels, such as {@code ""} or {@code " "}, reports no destinations.
     * Each label must exactly match a {@link DestinationType} label, otherwise this throws.
     */
    static Set<DestinationType> parseDestinationTypes(final String destinationTypesParam) {
        final Set<DestinationType> types = EnumSet.noneOf(DestinationType.class);
        for (final String token : (destinationTypesParam == null ? DEFAULT_DESTINATION_TYPES : destinationTypesParam).split(",")) {
            final String label = token.trim();
            if (!label.isEmpty()) {
                types.add(DestinationType.fromLabel(label));
            }
        }
        return types;
    }

    Set<DestinationType> getEnabledDestinationTypes() {
        return enabledDestinationTypes;
    }

    /**
     * Parses the {@code per_object} request parameter. Absent means {@code false}. Otherwise it must be given
     * exactly once, as {@code true} or {@code false}; anything else throws.
     */
    static boolean parsePerObject(final String[] values) {
        if (values == null) {
            return false;
        }
        if (values.length == 1) {
            if ("true".equals(values[0])) {
                return true;
            }
            if ("false".equals(values[0])) {
                return false;
            }
        }
        throw new IllegalArgumentException(PARAM_PER_OBJECT + " must be given once, as true or false");
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        // The request is only used to determine if the response needs to include per-object metrics.
        final boolean perObject;
        try {
            perObject = parsePerObject(request.getParameterValues(PARAM_PER_OBJECT));
        } catch (final IllegalArgumentException exception) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, exception.getMessage());
            return;
        }

        final MetricSnapshots snapshots;
        try {
            snapshots = collect(perObject);
        } catch (final RuntimeException exception) {
            LOG.warn("Prometheus scrape failed while querying broker MBeans", exception);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Metrics collection failed");
            return;
        }

        response.setContentType(writer.getContentType());
        response.setStatus(HttpServletResponse.SC_OK);
        final OutputStream out = response.getOutputStream();
        writer.write(out, snapshots);
        out.flush();
    }

    /**
     * Collects the metric families for one scrape. Unreadable attributes are reported as 0 so a partial
     * scrape still succeeds. Package-private so tests can render the snapshots through any exposition format.
     */
    MetricSnapshots collect(final boolean perObject) {
        final Map<BrokerViewMBean, Labels> brokerSeries = new LinkedHashMap<>();
        final Map<DestinationViewMBean, Labels> destinationSeries = new LinkedHashMap<>();
        for (final JmxBroker jmxBroker : brokers.get()) {
            final BrokerViewMBean broker = JMX.newMBeanProxy(jmxBroker.server, jmxBroker.name, BrokerViewMBean.class);
            // Skip brokers whose identity cannot be read rather than emit a phantom series.
            final String brokerName = readName(jmxBroker.name, broker::getBrokerName);
            if (brokerName == null) {
                continue;
            }
            brokerSeries.put(broker, Labels.of(LABEL_BROKER, brokerName));
            if (perObject) {
                // Scraping brokers with many destinations is expensive, so this is opt-in per request.
                addDestinations(jmxBroker, broker, brokerName, destinationSeries);
            }
        }

        final MetricSnapshots.Builder snapshots = MetricSnapshots.builder();
        addFamilies(snapshots, BROKER_METRIC_PREFIX, BROKER_METRICS, brokerSeries);
        // One family per metric; the destination type is a label, so the family set is fixed.
        addFamilies(snapshots, DESTINATION_METRIC_PREFIX, DESTINATION_METRICS, destinationSeries);
        return snapshots.build();
    }

    private void addDestinations(final JmxBroker jmxBroker, final BrokerViewMBean broker, final String brokerName,
            final Map<DestinationViewMBean, Labels> series) {
        for (final DestinationType type : enabledDestinationTypes) {
            final ObjectName[] names;
            try {
                names = type.list.apply(broker);
            } catch (final RuntimeException exception) {
                LOG.debug("Skipping {} destinations of {}: list unavailable", type.label, jmxBroker.name, exception);
                continue;
            }
            for (final ObjectName name : names) {
                final DestinationViewMBean destination = JMX.newMBeanProxy(jmxBroker.server, name, DestinationViewMBean.class);
                final String destinationName = readName(name, destination::getName);
                if (destinationName != null) {
                    series.put(destination, Labels.of(
                            LABEL_BROKER, brokerName,
                            LABEL_DESTINATION_TYPE, type.label,
                            LABEL_DESTINATION, destinationName));
                }
            }
        }
    }

    /** Brokers registered in this JVM that publish JMX MBeans. */
    static List<JmxBroker> registeredBrokers() {
        final BrokerRegistry registry = BrokerRegistry.getInstance();
        final List<BrokerService> services;
        synchronized (registry.getRegistryMutext()) {
            services = new ArrayList<>(registry.getBrokers().values());
        }
        final List<JmxBroker> result = new ArrayList<>();
        for (final BrokerService service : services) {
            if (!service.isUseJmx()) {
                continue;
            }
            try {
                result.add(new JmxBroker(service.getBrokerObjectName(), service.getManagementContext().getMBeanServer()));
            } catch (final Exception exception) {
                LOG.debug("Skipping broker {}: MBean name unavailable", service.getBrokerName(), exception);
            }
        }
        return result;
    }

    private static <T> void addFamilies(final MetricSnapshots.Builder snapshots, final String prefix,
            final List<MetricDefinition<T>> metrics, final Map<T, Labels> objects) {
        if (objects.isEmpty()) {
            return;
        }
        for (final MetricDefinition<T> metric : metrics) {
            snapshots.metricSnapshot(buildSnapshot(prefix + metric.name, metric, objects));
        }
    }

    // Builds one metric family (a gauge or counter) with a data point per object.
    private static <T> MetricSnapshot buildSnapshot(final String metricName, final MetricDefinition<T> metric,
            final Map<T, Labels> objects) {
        if (metric.type == MetricType.COUNTER) {
            final CounterSnapshot.Builder builder = CounterSnapshot.builder().name(metricName).help(metric.help);
            for (final Map.Entry<T, Labels> entry : objects.entrySet()) {
                builder.dataPoint(CounterDataPointSnapshot.builder()
                        .labels(entry.getValue())
                        .value(read(metric, entry.getKey(), entry.getValue()))
                        .build());
            }
            return builder.build();
        }

        final GaugeSnapshot.Builder builder = GaugeSnapshot.builder().name(metricName).help(metric.help);
        for (final Map.Entry<T, Labels> entry : objects.entrySet()) {
            builder.dataPoint(GaugeDataPointSnapshot.builder()
                    .labels(entry.getValue())
                    .value(read(metric, entry.getKey(), entry.getValue()))
                    .build());
        }
        return builder.build();
    }

    // Reads a broker or destination name used as a label. null means the object is skipped.
    private static String readName(final ObjectName name, final Supplier<String> getter) {
        try {
            return getter.get();
        } catch (final RuntimeException exception) {
            // The proxy wraps JMX failures (missing attribute, wrong type, unregistered MBean) in runtime exceptions.
            LOG.debug("Skipping object {}: name unavailable", name, exception);
            return null;
        }
    }

    private static <T> double read(final MetricDefinition<T> metric, final T mbean, final Labels labels) {
        try {
            return metric.value.applyAsDouble(mbean);
        } catch (final RuntimeException exception) {
            // Some attributes are not available on every ActiveMQ deployment (eg: bridge metrics).
            LOG.debug("Reporting 0 for {} on {}: attribute unavailable", metric.name, labels, exception);
            return 0;
        }
    }

    private static <T> MetricDefinition<T> gauge(final String name, final String help, final ToDoubleFunction<T> value) {
        return new MetricDefinition<>(name, help, value, MetricType.GAUGE);
    }

    private static <T> MetricDefinition<T> counter(final String name, final String help, final ToDoubleFunction<T> value) {
        return new MetricDefinition<>(name, help, value, MetricType.COUNTER);
    }

    private static final class MetricDefinition<T> {
        private final String name;
        private final String help;
        private final ToDoubleFunction<T> value;
        private final MetricType type;

        private MetricDefinition(final String name, final String help, final ToDoubleFunction<T> value, final MetricType type) {
            this.name = name;
            this.help = help;
            this.value = value;
            this.type = type;
        }
    }

    enum MetricType {
        GAUGE,
        COUNTER
    }
}
