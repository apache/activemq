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
package org.activemq.transport.discovery.multicast;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.activemq.transport.discovery.DiscoveryAgent;
import org.activemq.transport.discovery.DiscoveryAgentFactory;
import org.activemq.util.IOExceptionSupport;
import org.activemq.util.IntrospectionSupport;
import org.activemq.util.URISupport;

public class MulticastDiscoveryAgentFactory extends DiscoveryAgentFactory {

    protected DiscoveryAgent doCreateDiscoveryAgent(URI uri) throws IOException {
        try {
            
            Map options = URISupport.parseParamters(uri);
            MulticastDiscoveryAgent rc = new MulticastDiscoveryAgent();
            rc.setGroup(uri.getHost());
            IntrospectionSupport.setProperties(rc, options);
            return rc;
            
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create discovery agent: " + uri, e);
        }
    }
}
