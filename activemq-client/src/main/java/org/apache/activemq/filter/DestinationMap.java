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
package org.apache.activemq.filter;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * A Map-like data structure allowing values to be indexed by
 * {@link ActiveMQDestination} and retrieved by destination - supporting both *
 * and &gt; style of wildcard as well as composite destinations. <br>
 * This class assumes that the index changes rarely but that fast lookup into
 * the index is required. So this class maintains a pre-calculated index for
 * destination steps. So looking up the values for "TEST.*" or "*.TEST" will be
 * pretty fast. <br>
 * Looking up of a value could return a single value or a List of matching
 * values if a wildcard or composite destination is used.
 */
public class DestinationMap {
    protected static final String ANY_DESCENDENT = DestinationFilter.ANY_DESCENDENT;
    protected static final String ANY_CHILD = DestinationFilter.ANY_CHILD;

    private DestinationMapNode queueRootNode = new DestinationMapNode(null);
    private DestinationMapNode tempQueueRootNode = new DestinationMapNode(null);
    private DestinationMapNode topicRootNode = new DestinationMapNode(null);
    private DestinationMapNode tempTopicRootNode = new DestinationMapNode(null);

    /**
     * Looks up the value(s) matching the given Destination key. For simple
     * destinations this is typically a List of one single value, for wildcards
     * or composite destinations this will typically be a List of matching
     * values.
     *
     * @param key the destination to lookup
     * @return a List of matching values or an empty list if there are no
     *         matching values.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public synchronized Set get(ActiveMQDestination key) {
        if (key.isComposite()) {
            ActiveMQDestination[] destinations = key.getCompositeDestinations();
            Set answer = new HashSet(destinations.length);
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination childDestination = destinations[i];
                Object value = get(childDestination);
                if (value instanceof Set) {
                    answer.addAll((Set) value);
                } else if (value != null) {
                    answer.add(value);
                }
            }
            return answer;
        }
        return findWildcardMatches(key);
    }

    public synchronized void put(ActiveMQDestination key, Object value) {
        if (key.isComposite()) {
            ActiveMQDestination[] destinations = key.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination childDestination = destinations[i];
                put(childDestination, value);
            }
            return;
        }
        String[] paths = key.getDestinationPaths();
        getRootNode(key).add(paths, 0, value);
    }


    /**
     * Removes the value from the associated destination
     */
    public synchronized void remove(ActiveMQDestination key, Object value) {
        if (key.isComposite()) {
            ActiveMQDestination[] destinations = key.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                ActiveMQDestination childDestination = destinations[i];
                remove(childDestination, value);
            }
            return;
        }
        String[] paths = key.getDestinationPaths();
        getRootNode(key).remove(paths, 0, value);

    }

    public int getTopicRootChildCount() {
        return topicRootNode.getChildCount();
    }

    public int getQueueRootChildCount() {
        return queueRootNode.getChildCount();
    }

    public DestinationMapNode getQueueRootNode() {
        return queueRootNode;
    }

    public DestinationMapNode getTopicRootNode() {
        return topicRootNode;
    }

    public DestinationMapNode getTempQueueRootNode() {
        return tempQueueRootNode;
    }

    public DestinationMapNode getTempTopicRootNode() {
        return tempTopicRootNode;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * A helper method to allow the destination map to be populated from a
     * dependency injection framework such as Spring
     */
    @SuppressWarnings({"rawtypes"})
    protected void setEntries(List<DestinationMapEntry> entries) {
        for (Object element : entries) {
            Class<? extends DestinationMapEntry> type = getEntryClass();
            if (type.isInstance(element)) {
                DestinationMapEntry entry = (DestinationMapEntry) element;
                put(entry.getDestination(), entry.getValue());
            } else {
                throw new IllegalArgumentException("Each entry must be an instance of type: " + type.getName() + " but was: " + element);
            }
        }
    }

    /**
     * Returns the type of the allowed entries which can be set via the
     * {@link #setEntries(List)} method. This allows derived classes to further
     * restrict the type of allowed entries to make a type safe destination map
     * for custom policies.
     */
    @SuppressWarnings({"rawtypes"})
    protected Class<? extends DestinationMapEntry> getEntryClass() {
        return DestinationMapEntry.class;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Set findWildcardMatches(ActiveMQDestination key) {
       return findWildcardMatches(key, true);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Set findWildcardMatches(ActiveMQDestination key, boolean deep) {
        String[] paths = key.getDestinationPaths();
        Set answer = new HashSet();
        getRootNode(key).appendMatchingValues(answer, paths, 0, deep);
        return answer;
    }

    /**
     * @param key
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Set removeAll(ActiveMQDestination key) {
        Set rc = new HashSet();
        if (key.isComposite()) {
            ActiveMQDestination[] destinations = key.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                rc.add(removeAll(destinations[i]));
            }
            return rc;
        }
        String[] paths = key.getDestinationPaths();
        getRootNode(key).removeAll(rc, paths, 0);
        return rc;
    }

    /**
     * Returns the value which matches the given destination or null if there is
     * no matching value. If there are multiple values, the results are sorted
     * and the last item (the biggest) is returned.
     *
     * @param destination the destination to find the value for
     * @return the largest matching value or null if no value matches
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Object chooseValue(final ActiveMQDestination destination) {
        Set set = get(destination);
        if (set == null || set.isEmpty()) {
            return null;
        }
        SortedSet sortedSet = new TreeSet(new Comparator<DestinationMapEntry>() {
            @Override
            public int compare(DestinationMapEntry entry1, DestinationMapEntry entry2) {
                return destination.equals(entry1.destination) ? -1 : (destination.equals(entry2.destination) ? 1 : entry1.compareTo(entry2));
            }
        });
        sortedSet.addAll(set);
        return sortedSet.first();
    }

    /**
     * Returns the root node for the given destination type
     */
    protected DestinationMapNode getRootNode(ActiveMQDestination key) {
        if (key.isTemporary()) {
            if (key.isQueue()) {
                return tempQueueRootNode;
            } else {
                return tempTopicRootNode;
            }
        } else {
            if (key.isQueue()) {
                return queueRootNode;
            } else {
                return topicRootNode;
            }
        }
    }

    public void reset() {
        queueRootNode = new DestinationMapNode(null);
        tempQueueRootNode = new DestinationMapNode(null);
        topicRootNode = new DestinationMapNode(null);
        tempTopicRootNode = new DestinationMapNode(null);
    }

    public boolean isEmpty() {
        return queueRootNode.isEmpty() && topicRootNode.isEmpty() && tempQueueRootNode.isEmpty() && tempTopicRootNode.isEmpty();
    }

    public static Set union(Set existing, Set candidates) {
        if (candidates != null) {
            if (existing != null) {
                for (Iterator<Object> iterator = existing.iterator(); iterator.hasNext(); ) {
                    Object toMatch = iterator.next();
                    if (!candidates.contains(toMatch)) {
                        iterator.remove();
                    }
                }
            } else {
                existing = candidates;
            }
        } else if (existing != null) {
            existing.clear();
        }
        return existing;
    }

}
