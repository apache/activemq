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

import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface AbortSlowConsumerStrategyViewMBean {
    
    @MBeanInfo("returns the current max slow count, -1 disables")
    long getMaxSlowCount();

    @MBeanInfo("sets the count after which a slow consumer will be aborted, -1 disables")
    void setMaxSlowCount(long maxSlowCount);
    
    @MBeanInfo("returns the current max slow (milliseconds) duration")
    long getMaxSlowDuration();

    @MBeanInfo("sets the duration (milliseconds) after which a continually slow consumer will be aborted")
    void setMaxSlowDuration(long maxSlowDuration);

    @MBeanInfo("returns the check period at which a sweep of consumers is done to determine continued slowness")
    public long getCheckPeriod();
    
    @MBeanInfo("returns the current list of slow consumers, Not HTML friendly")
    TabularData getSlowConsumers() throws OpenDataException;
    
    @MBeanInfo("aborts the slow consumer gracefully by sending a shutdown control message to just that consumer")
    void abortConsumer(ObjectName consumerToAbort);
    
    @MBeanInfo("aborts the slow consumer forcefully by shutting down it's connection, note: all other users of the connection will be affected")
    void abortConnection(ObjectName consumerToAbort);

    @MBeanInfo("aborts the slow consumer gracefully by sending a shutdown control message to just that consumer")
    void abortConsumer(String objectNameOfConsumerToAbort);
    
    @MBeanInfo("aborts the slow consumer forcefully by shutting down it's connection, note: all other users of the connection will be affected")
    void abortConnection(String objectNameOfConsumerToAbort);
}
