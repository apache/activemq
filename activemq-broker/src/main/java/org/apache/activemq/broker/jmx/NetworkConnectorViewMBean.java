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

import org.apache.activemq.Service;

public interface NetworkConnectorViewMBean extends Service {

    String getName();

    int getMessageTTL();

    int getConsumerTTL();

    int getPrefetchSize();

    /**
     * @return Advisory prefetch setting.
     */
    @MBeanInfo("The prefetch setting for the advisory message consumer.  If set to <= 0 then this setting is disabled "
            + "and the prefetchSize attribute is used instead for configuring the advisory consumer.")
    int getAdvisoryPrefetchSize();

    String getUserName();

    boolean isBridgeTempDestinations();

    boolean isConduitSubscriptions();

    boolean isDecreaseNetworkConsumerPriority();

    boolean isDispatchAsync();

    boolean isDynamicOnly();

    boolean isDuplex();

    boolean isSuppressDuplicateQueueSubscriptions();

    boolean isSuppressDuplicateTopicSubscriptions();

    void setBridgeTempDestinations(boolean bridgeTempDestinations);

    void setConduitSubscriptions(boolean conduitSubscriptions);

    void setDispatchAsync(boolean dispatchAsync);

    void setDynamicOnly(boolean dynamicOnly);

    void setMessageTTL(int messageTTL);

    void setConsumerTTL(int consumerTTL);

    void setPassword(String password);

    void setPrefetchSize(int prefetchSize);

    void setAdvisoryPrefetchSize(int advisoryPrefetchSize);

    void setUserName(String userName);

    String getPassword();

    void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority);

    void setSuppressDuplicateQueueSubscriptions(boolean val);

    void setSuppressDuplicateTopicSubscriptions(boolean val);

    String getRemoteUserName();

    void setRemoteUserName(String remoteUserName);

    String getRemotePassword();

    void setRemotePassword(String remotePassword);

}
