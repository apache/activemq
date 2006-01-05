/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq;

/**
 * Configuration options used to control how messages are re-delivered when they
 * are rolled back.
 * 
 * @version $Revision: 1.11 $
 */
public class RedeliveryPolicy implements Cloneable {

    protected int maximumRedeliveries = 5;
    protected long initialRedeliveryDelay = 1000L;
    protected boolean useExponentialBackOff = false;
    protected short backOffMultiplier = 5;

    public RedeliveryPolicy() {
    }

    public RedeliveryPolicy copy() {
        try {
            return (RedeliveryPolicy) clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException("Could not clone: " + e, e);
        }
    }

    public short getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public void setBackOffMultiplier(short backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    public long getInitialRedeliveryDelay() {
        return initialRedeliveryDelay;
    }

    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        this.initialRedeliveryDelay = initialRedeliveryDelay;
    }

    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }
}
