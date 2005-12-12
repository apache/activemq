/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.command;

/**
 * Configuration options used to control how messages are re-delivered when
 * they are rolled back.
 *  
 * @openwire:marshaller
 * @version $Revision: 1.11 $
 */
public class RedeliveryPolicy implements DataStructure {
    
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.REDELIVERY_POLICY;
    
    protected int maximumRedeliveries = 5;
    protected long initialRedeliveryDelay = 1000L;
    protected boolean useExponentialBackOff = false;
    protected short backOffMultiplier = 5;
    
    public RedeliveryPolicy() {        
    }    
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=false
     */
    public short getBackOffMultiplier() {
        return backOffMultiplier;
    }
    public void setBackOffMultiplier(short backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    /**
     * @openwire:property version=1 cache=false
     */
    public long getInitialRedeliveryDelay() {
        return initialRedeliveryDelay;
    }
    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        this.initialRedeliveryDelay = initialRedeliveryDelay;
    }

    /**
     * @openwire:property version=1 cache=false
     */
    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }
    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    /**
     * @openwire:property version=1 cache=false
     */
    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }
    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    public boolean isMarshallAware() {
        return false;
    }
}
