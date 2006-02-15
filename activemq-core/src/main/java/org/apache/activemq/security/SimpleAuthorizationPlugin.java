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
package org.apache.activemq.security;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.filter.DestinationMap;

/**
 * A simple authorization plugin
 * 
 * @org.apache.xbean.XBean element="simpleAuthorizationPlugin" description="Provides a simple authorization
 * plugin where each ACL is a destination map of destinations to role names"
 *
 * @version $Revision$
 */
public class SimpleAuthorizationPlugin implements BrokerPlugin {

    private DestinationMap writeACLs;
    private DestinationMap readACLs;
    private DestinationMap adminACLs;

    public Broker installPlugin(Broker broker) {
        return new SimpleAuthorizationBroker(broker, writeACLs, readACLs, adminACLs);
    }

    public DestinationMap getAdminACLs() {
        return adminACLs;
    }

    public void setAdminACLs(DestinationMap adminACLs) {
        this.adminACLs = adminACLs;
    }

    public DestinationMap getReadACLs() {
        return readACLs;
    }

    public void setReadACLs(DestinationMap readACLs) {
        this.readACLs = readACLs;
    }

    public DestinationMap getWriteACLs() {
        return writeACLs;
    }

    public void setWriteACLs(DestinationMap writeACLs) {
        this.writeACLs = writeACLs;
    }

}
