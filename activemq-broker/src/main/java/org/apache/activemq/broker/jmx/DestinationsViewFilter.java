/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import static org.apache.activemq.util.IntrospectionSupport.findGetterMethod;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Defines a query API for destinations MBeans
 *
 * Typical usage
 *
 *         return DestinationsViewFilter.create(filter)
 *                .setDestinations(broker.getQueueViews())
 *                .filter(page, pageSize);
 *
 * where 'filter' is JSON representation of the query, like
 *
 * {name: '77', filter:'nonEmpty', sortColumn:'queueSize', sortOrder:'desc'}
 *
 * This returns a JSON map, containing filtered map of MBeans in the "data" field and total number of destinations that match criteria in the "count" field.
 * The result will be properly paged, according to 'page' and 'pageSize' parameters.
 *
 */
public class DestinationsViewFilter implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DestinationsViewFilter.class);

    private static final long serialVersionUID = 1L;

    /**
     * Name pattern used to filter destinations
     */
    String name;

    /**
     * Arbitrary filter key to be applied to the destinations. Currently only simple predefined filters has been implemented:
     *
     * empty - return only empty queues (queueSize = 0)
     * nonEmpty - return only non-empty queues queueSize != 0)
     * noConsumer - return only destinations that doesn't have consumers
     * nonAdvisory - return only non-Advisory topics
     *
     * For more implementation details see {@link DestinationsViewFilter.getPredicate}
     *
     */
    String filter;

    /**
     * Sort destinations by this {@link DestinationView} property
     */
    String sortColumn = "name";

    /**
     * Order of sorting - 'asc' or 'desc'
     */
    String sortOrder = "asc";

    Map<ObjectName, DestinationView> destinations;


    public DestinationsViewFilter() {
    }

    /**
     * Creates an object from the JSON string
     *
     */
    public static DestinationsViewFilter create(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        if (json == null) {
            return new DestinationsViewFilter();
        }
        json = json.trim();
        if (json.length() == 0 || json.equals("{}")) {
            return new DestinationsViewFilter();
        }
        return mapper.readerFor(DestinationsViewFilter.class).readValue(json);
    }

    /**
     * Destination MBeans to be queried
     */
    public DestinationsViewFilter setDestinations(Map<ObjectName, DestinationView> destinations) {
        this.destinations = destinations;
        return this;
    }

    /**
     * Filter, sort and page results.
     *
     * Returns JSON map with resulting destination views and total number of matched destinations
     *
     * @param page - defines result page to be returned
     * @param pageSize - defines page size to be used
     * @throws IOException
     */
    String filter(int page, int pageSize) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final Predicate<DestinationView> predicate = getPredicate();
        final Map<ObjectName, DestinationView> filteredDestinations = new HashMap<>(destinations);
        destinations = filteredDestinations.entrySet().stream().filter(d -> predicate.test(d.getValue())).collect(
                Collectors.toMap(d -> d.getKey(), d -> d.getValue()));

        Map<ObjectName, DestinationView> pagedDestinations = getPagedDestinations(page, pageSize);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("data", pagedDestinations);
        result.put("count", destinations.size());
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, result);
        return writer.toString();
    }

    Map<ObjectName, DestinationView> getPagedDestinations(int page, int pageSize) {
        int start = (page - 1) * pageSize;
        int end = Math.min(page * pageSize, destinations.size());

        final List<Map.Entry<ObjectName, DestinationView>> sortedCopy = new ArrayList<>(destinations.entrySet());
        sortedCopy.sort(getComparator());
        return sortedCopy.subList(start, end).stream().collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    Predicate<DestinationView> getPredicate() {
        return new Predicate<DestinationView>() {
            @Override
            public boolean test(DestinationView input) {
                boolean match = true;
                if (getName() != null && !getName().isEmpty()) {
                    match = input.getName().contains(getName());
                }

                if (match) {
                    if (getFilter().equals("empty")) {
                        match = input.getQueueSize() == 0;
                    }
                    if (getFilter().equals("nonEmpty")) {
                        match = input.getQueueSize() != 0;
                    }
                    if (getFilter().equals("noConsumer")) {
                        match = input.getConsumerCount() == 0;
                    }
                    if (getFilter().equals("nonAdvisory")) {
                        return !(input instanceof TopicView && AdvisorySupport.isAdvisoryTopic(new ActiveMQTopic(input.getName())));
                    }
                }

                return match;
            }
        };
    }

    Comparator<Map.Entry<ObjectName, DestinationView>> getComparator() {
        return new Comparator<Map.Entry<ObjectName, DestinationView>>() {

            Method getter = findGetterMethod(DestinationView.class, getSortColumn());

            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override
            public int compare(Map.Entry<ObjectName, DestinationView> left, Map.Entry<ObjectName, DestinationView> right) {
                try {
                    if (getter != null) {
                        Object leftValue = getter.invoke(left.getValue());
                        Object rightValue = getter.invoke(right.getValue());
                        if (leftValue instanceof Comparable && rightValue instanceof Comparable) {
                            if (getSortOrder().equalsIgnoreCase("desc")) {
                                return ((Comparable) rightValue).compareTo(leftValue);
                            } else {
                                return ((Comparable) leftValue).compareTo(rightValue);
                            }
                        }
                    }
                    return 0;
                } catch (Exception e) {
                    LOG.info("Exception sorting destinations", e);
                    return 0;
                }
            }
        };
    }

    public Map<ObjectName, DestinationView> getDestinations() {
        return destinations;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    public String getSortColumn() {
        return sortColumn;
    }

    public void setSortColumn(String sortColumn) {
        this.sortColumn = sortColumn;
    }
}
