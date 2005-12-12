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

import org.activemq.state.CommandVisitor;

/**
 * 
 * @openwire:marshaller
 * @version $Revision: 1.7 $
 */
public class RemoveSubscriptionInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.REMOVE_SUBSCRIPTION_INFO;

    protected ConnectionId connectionId;
    protected String clientId;
    protected String subcriptionName;
    

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }
    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * @openwire:property version=1
     */
    public String getSubcriptionName() {
        return subcriptionName;
    }

    public void setSubcriptionName(String subcriptionName) {
        this.subcriptionName = subcriptionName;
    }

    /**
     * @openwire:property version=1
     */
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Response visit(CommandVisitor visitor) throws Throwable {
        return visitor.processRemoveSubscription( this );
    }

}
