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

/**
 * Configuration keys, metric name prefixes and label names used by the Prometheus metrics endpoint.
 */
public final class PrometheusConstants {

    /** Request parameter that enables per-destination series. Accepted values: {@code true} or {@code false}, given once. */
    public static final String PARAM_PER_OBJECT = "per_object";

    /** Init parameter: comma separated destination types reported when {@link #PARAM_PER_OBJECT} is set. */
    public static final String INIT_PARAM_DESTINATION_TYPES = "destinationTypes";
    public static final String DEFAULT_DESTINATION_TYPES = "queue,topic";

    public static final String BROKER_METRIC_PREFIX = "activemq_broker_";
    public static final String DESTINATION_METRIC_PREFIX = "activemq_destination_";

    public static final String LABEL_BROKER = "broker";
    public static final String LABEL_DESTINATION_TYPE = "destination_type";
    public static final String LABEL_DESTINATION = "destination";

    private PrometheusConstants() {
    }
}
