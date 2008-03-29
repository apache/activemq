/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.discovery.multicast;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryAgentFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;

public class MulticastDiscoveryAgentFactory extends DiscoveryAgentFactory {
    
    //See AMQ-1489. There's something wrong here but it is difficult to tell what.
    //It looks like to actually set the discovery URI you have to use something like
    //<transportConnector uri="..." discoveryUri="multicast://239.3.7.0:37000?discoveryURI=multicast://239.3.7.0:37000" />
    // or
    //<networkConnector name="..." uri="multicast://239.3.7.0:37000?discoveryURI=multicast://239.3.7.0:37000">
    protected DiscoveryAgent doCreateDiscoveryAgent(URI uri) throws IOException {
        try {
            
            Map options = URISupport.parseParamters(uri);
            MulticastDiscoveryAgent rc = new MulticastDiscoveryAgent();
            rc.setGroup(uri.getHost());

            // allow the discoveryURI to be set via a query argument on the URI
            // ?discoveryURI=someURI
            IntrospectionSupport.setProperties(rc, options);
            return rc;
            
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create discovery agent: " + uri, e);
        }
    }
}
