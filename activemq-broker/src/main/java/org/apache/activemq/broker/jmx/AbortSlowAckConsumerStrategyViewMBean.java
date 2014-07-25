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

public interface AbortSlowAckConsumerStrategyViewMBean extends AbortSlowConsumerStrategyViewMBean {

    @MBeanInfo("returns the current max time since last ack setting")
    long getMaxTimeSinceLastAck();

    @MBeanInfo("sets the duration (milliseconds) after which a consumer that doesn't ack a message will be marked as slow")
    void setMaxTimeSinceLastAck(long maxTimeSinceLastAck);

    @MBeanInfo("returns the current value of the ignore idle consumers setting.")
    boolean isIgnoreIdleConsumers();

    @MBeanInfo("sets whether consumers that are idle (no dispatched messages) should be included when checking for slow acks.")
    void setIgnoreIdleConsumers(boolean ignoreIdleConsumers);

    @MBeanInfo("returns the current value of the ignore network connector consumers setting.")
    boolean isIgnoreNetworkConsumers();

    @MBeanInfo("sets whether consumers that are from network connector should be included when checking for slow acks.")
    void setIgnoreNetworkConsumers(boolean ignoreIdleConsumers);

}
