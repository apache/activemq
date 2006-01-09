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
package org.apache.activemq.command;


/**
 * Represents a discovery event containing the details of the service
 *
 * @openwire:marshaller code="55"
 * @version $Revision:$
 */
public class DiscoveryEvent implements DataStructure {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.DISCOVERY_EVENT;

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    protected String serviceName;
    protected String brokerName;

    public DiscoveryEvent(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * @openwire:property version=1
     */
    public String getServiceName() {
        return serviceName;
    }
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
    
    /**
     * @openwire:property version=1
     */
    public String getBrokerName(){
        return brokerName;
    }
    public void setBrokerName(String name){
        this.brokerName = name;
    }

    public boolean isMarshallAware() {
        return false;
    }    
}
