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
package org.apache.activemq.broker.jmx;

import java.util.List;
import java.util.Map;

import javax.jms.InvalidSelectorException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface DestinationViewMBean {

    /**
     * Returns the name of this destination
     */
    String getName();

    /**
     * Resets the managment counters.
     */
    void resetStatistics();

    /**
     * Returns the number of messages that have been sent to the destination.
     * 
     * @return The number of messages that have been sent to the destination.
     */
    long getEnqueueCount();

    /**
     * Returns the number of messages that have been delivered (potentially not
     * acknowledged) to consumers.
     * 
     * @return The number of messages that have been delivered (potentially not
     *         acknowledged) to consumers.
     */
    long getDispatchCount();

    /**
     * Returns the number of messages that have been acknowledged from the
     * destination.
     * 
     * @return The number of messages that have been acknowledged from the
     *         destination.
     */
    long getDequeueCount();

    /**
     * Returns the number of consumers subscribed this destination.
     * 
     * @return The number of consumers subscribed this destination.
     */
    long getConsumerCount();

    /**
     * Returns the number of messages in this destination which are yet to be
     * consumed
     * 
     * @return Returns the number of messages in this destination which are yet
     *         to be consumed
     */
    long getQueueSize();

    /**
     * @return An array of all the messages in the destination's queue.
     */
    CompositeData[] browse() throws OpenDataException;

    /**
     * @return A list of all the messages in the destination's queue.
     */
    TabularData browseAsTable() throws OpenDataException;

    /**
     * @return An array of all the messages in the destination's queue.
     * @throws InvalidSelectorException
     */
    CompositeData[] browse(String selector) throws OpenDataException, InvalidSelectorException;

    /**
     * @return A list of all the messages in the destination's queue.
     * @throws InvalidSelectorException
     */
    TabularData browseAsTable(String selector) throws OpenDataException, InvalidSelectorException;

    /**
     * Sends a TextMesage to the destination.
     * 
     * @param body the text to send
     * @return the message id of the message sent.
     * @throws Exception
     */
    String sendTextMessage(String body) throws Exception;

    /**
     * Sends a TextMesage to the destination.
     * 
     * @param headers the message headers and properties to set. Can only
     *                container Strings maped to primitive types.
     * @param body the text to send
     * @return the message id of the message sent.
     * @throws Exception
     */
    String sendTextMessage(Map headers, String body) throws Exception;

    int getMemoryPercentageUsed();

    long getMemoryLimit();

    void setMemoryLimit(long limit);

    /**
     * Browses the current destination returning a list of messages
     */
    List browseMessages() throws InvalidSelectorException;

    /**
     * Browses the current destination with the given selector returning a list
     * of messages
     */
    List browseMessages(String selector) throws InvalidSelectorException;

    /**
     * @return longest time a message is held by a destination
     */
    long getMaxEnqueueTime();

    /**
     * @return shortest time a message is held by a destination
     */
    long getMinEnqueueTime();

    /**
     * @return average time a message is held by a destination
     */
    double getAverageEnqueueTime();

}
