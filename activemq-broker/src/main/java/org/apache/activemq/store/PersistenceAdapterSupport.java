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
package org.apache.activemq.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * Used to implement common PersistenceAdapter methods.
 */
public class PersistenceAdapterSupport {

    private static final DestinationMatcher MATCH_ALL = new AlwaysMatches();

    /**
     * Provides an interface for a Destination matching object that can be used to
     * search for specific destinations from a persistence adapter.
     */
    public interface DestinationMatcher {

        /**
         * Given a Destination object, return true if the destination matches some defined
         * search criteria, false otherwise.
         *
         * @param destination
         *        the destination to inspect.
         *
         * @return true if the destination matches the target criteria, false otherwise.
         */
        boolean matches(ActiveMQDestination destination);

    }

    /**
     * Searches the set of subscriptions from the given persistence adapter and returns all those
     * that belong to the given ClientId value.
     *
     * @param adapter
     *        the persistence adapter instance to search within.
     * @param clientId
     *        the client ID value used to filter the subscription set.
     *
     * @return a list of all subscriptions belonging to the given client.
     *
     * @throws IOException if an error occurs while listing the stored subscriptions.
     */
    static public List<SubscriptionInfo> listSubscriptions(PersistenceAdapter adapter, String clientId) throws IOException {
        ArrayList<SubscriptionInfo> rc = new ArrayList<SubscriptionInfo>();
        for (ActiveMQDestination destination : adapter.getDestinations()) {
            if (destination.isTopic()) {
                TopicMessageStore store = adapter.createTopicMessageStore((ActiveMQTopic) destination);
                for (SubscriptionInfo sub : store.getAllSubscriptions()) {
                    if (clientId == sub.getClientId() || clientId.equals(sub.getClientId())) {
                        rc.add(sub);
                    }
                }
            }
        }
        return rc;
    }

    /**
     * Provides a means of querying the persistence adapter for a list of ActiveMQQueue instances.
     *
     * @param adapter
     *        the persistence adapter instance to query.
     *
     * @return a List<ActiveMQQeue> with all the queue destinations.
     *
     * @throws IOException if an error occurs while reading the destinations.
     */
    static public List<ActiveMQQueue> listQueues(PersistenceAdapter adapter) throws IOException {
        return listQueues(adapter, MATCH_ALL);
    }

    /**
     * Provides a means of querying the persistence adapter for a list of ActiveMQQueue instances
     * that match some given search criteria.
     *
     * @param adapter
     *        the persistence adapter instance to query.
     * @param matcher
     *        the DestinationMatcher instance used to find the target destinations.
     *
     * @return a List<ActiveMQQeue> with all the matching destinations.
     *
     * @throws IOException if an error occurs while reading the destinations.
     */
    static public List<ActiveMQQueue> listQueues(PersistenceAdapter adapter, DestinationMatcher matcher) throws IOException {
        ArrayList<ActiveMQQueue> rc = new ArrayList<ActiveMQQueue>();
        for (ActiveMQDestination destination : adapter.getDestinations()) {
            if (destination.isQueue() && matcher.matches(destination)) {
                rc.add((ActiveMQQueue) destination);
            }
        }
        return rc;
    }

    /**
     * Provides a means of querying the persistence adapter for a list of ActiveMQTopic instances.
     *
     * @param adapter
     *        the persistence adapter instance to query.
     *
     * @return a List<ActiveMQTopic> with all the topic destinations.
     *
     * @throws IOException if an error occurs while reading the destinations.
     */
    static public List<ActiveMQTopic> listTopics(PersistenceAdapter adapter) throws IOException {
        return listTopics(adapter, MATCH_ALL);
    }

    /**
     * Provides a means of querying the persistence adapter for a list of ActiveMQTopic instances
     * that match some given search criteria.
     *
     * @param adapter
     *        the persistence adapter instance to query.
     * @param matcher
     *        the DestinationMatcher instance used to find the target destinations.
     *
     * @return a List<ActiveMQTopic> with all the matching destinations.
     *
     * @throws IOException if an error occurs while reading the destinations.
     */
    static public List<ActiveMQTopic> listTopics(PersistenceAdapter adapter, DestinationMatcher matcher) throws IOException {
        ArrayList<ActiveMQTopic> rc = new ArrayList<ActiveMQTopic>();
        for (ActiveMQDestination destination : adapter.getDestinations()) {
            if (destination.isTopic() && matcher.matches(destination)) {
                rc.add((ActiveMQTopic) destination);
            }
        }
        return rc;
    }

    private static class AlwaysMatches implements DestinationMatcher {

        @Override
        public boolean matches(ActiveMQDestination destination) {
            return true;
        }
    }
}
