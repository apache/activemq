/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.Service;

public interface ConnectorViewMBean extends Service {

    public short getBackOffMultiplier();

    public long getInitialRedeliveryDelay();

    public int getMaximumRedeliveries();

    public boolean isUseExponentialBackOff();

    public void setBackOffMultiplier(short backOffMultiplier);

    public void setInitialRedeliveryDelay(long initialRedeliveryDelay);

    public void setMaximumRedeliveries(int maximumRedeliveries);

    public void setUseExponentialBackOff(boolean useExponentialBackOff);
    
    /**
     * Resets the statistics
     */
    public void resetStatistics();

    /**
     * Returns the number of messages enqueued on this connector
     * 
     * @return the number of messages enqueued on this connector
     */
    public long getEnqueueCount();

    /**
     * Returns the number of messages dequeued on this connector
     * 
     * @return the number of messages dequeued on this connector
     */
    public long getDequeueCount();

}