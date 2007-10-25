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
package org.apache.activemq.broker.region;


/**
 * @version $Revision: 1.12 $
 */
public abstract class BaseDestination implements Destination {

    private boolean producerFlowControl = true;
    private int maxProducersToAudit=1024;
    private int maxAuditDepth=1;
    private boolean enableAudit=true;
    /**
     * @return the producerFlowControl
     */
    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }
    /**
     * @param producerFlowControl the producerFlowControl to set
     */
    public void setProducerFlowControl(boolean producerFlowControl) {
        this.producerFlowControl = producerFlowControl;
    }
    /**
     * @return the maxProducersToAudit
     */
    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }
    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
    }
    /**
     * @return the maxAuditDepth
     */
    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }
    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }
    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit() {
        return enableAudit;
    }
    /**
     * @param enableAudit the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    
}
