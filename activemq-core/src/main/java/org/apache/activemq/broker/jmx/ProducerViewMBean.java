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

public interface ProducerViewMBean {

    /**
     * @return the clientId of the Connection the Producer is on
     */
    @MBeanInfo("JMS Client id of the Connection the Producer is on.")
    String getClientId();

    /**
     * @return the id of the Connection the Producer is on
     */
    @MBeanInfo("ID of the Connection the Producer is on.")
    String getConnectionId();

    /**
     * @return the id of the Session the Producer is on
     */
    @MBeanInfo("ID of the Session the Producer is on.")
    long getSessionId();

    /**
     * @return the id of Producer.
     */
    @MBeanInfo("ID of the Producer.")
    String getProducerId();

    /**
     * @return the destination name
     */
    @MBeanInfo("The name of the destionation the Producer is on.")
    String getDestinationName();

    /**
     * @return true if the destination is a Queue
     */
    @MBeanInfo("Producer is on a Queue")
    boolean isDestinationQueue();

    /**
     * @return true of the destination is a Topic
     */
    @MBeanInfo("Producer is on a Topic")
    boolean isDestinationTopic();

    /**
     * @return true if the destination is temporary
     */
    @MBeanInfo("Producer is on a temporary Queue/Topic")
    boolean isDestinationTemporary();

    /**
     * @returns the windows size configured for the producer
     */
    @MBeanInfo("Configured Window Size for the Producer")
    int getProducerWindowSize();

    /**
     * @returns if the Producer is configured for Async dispatch
     */
    @MBeanInfo("Is the producer configured for Async Dispatch")
    boolean isDispatchAsync();
}
