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
package org.apache.activemq.transport.discovery;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;

public abstract class DiscoveryAgentFactory {

    private static final FactoryFinder DISCOVERY_AGENT_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/discoveryagent/");
    private static final ConcurrentMap<String, DiscoveryAgentFactory> DISCOVERY_AGENT_FACTORYS = new ConcurrentHashMap<String, DiscoveryAgentFactory>();

    /**
     * @param uri
     * @return
     * @throws IOException
     */
    private static DiscoveryAgentFactory findDiscoveryAgentFactory(URI uri) throws IOException {
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new IOException("DiscoveryAgent scheme not specified: [" + uri + "]");
        }
        DiscoveryAgentFactory daf = DISCOVERY_AGENT_FACTORYS.get(scheme);
        if (daf == null) {
            // Try to load if from a META-INF property.
            try {
                daf = (DiscoveryAgentFactory)DISCOVERY_AGENT_FINDER.newInstance(scheme);
                DISCOVERY_AGENT_FACTORYS.put(scheme, daf);
            } catch (Throwable e) {
                throw IOExceptionSupport.create("DiscoveryAgent scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return daf;
    }

    public static DiscoveryAgent createDiscoveryAgent(URI uri) throws IOException {
        DiscoveryAgentFactory tf = findDiscoveryAgentFactory(uri);
        return tf.doCreateDiscoveryAgent(uri);

    }

    protected abstract DiscoveryAgent doCreateDiscoveryAgent(URI uri) throws IOException;
    // {
    // try {
    // String type = ( uri.getScheme() == null ) ? uri.getPath() :
    // uri.getScheme();
    // DiscoveryAgent rc = (DiscoveryAgent)
    // discoveryAgentFinder.newInstance(type);
    // Map options = URISupport.parseParamters(uri);
    // IntrospectionSupport.setProperties(rc, options);
    // if( rc.getClass() == SimpleDiscoveryAgent.class ) {
    // CompositeData data = URISupport.parseComposite(uri);
    // ((SimpleDiscoveryAgent)rc).setServices(data.getComponents());
    // }
    // return rc;
    // } catch (Throwable e) {
    // throw IOExceptionSupport.create("Could not create discovery agent: "+uri,
    // e);
    // }
    // }
}
