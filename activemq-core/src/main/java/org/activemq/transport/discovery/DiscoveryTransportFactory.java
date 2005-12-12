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
package org.activemq.transport.discovery;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.activemq.transport.Transport;
import org.activemq.transport.TransportServer;
import org.activemq.transport.failover.FailoverTransportFactory;
import org.activemq.util.IntrospectionSupport;
import org.activemq.util.URISupport.CompositeData;

/**
 * @version $Revision$
 */
public class DiscoveryTransportFactory extends FailoverTransportFactory {
        
    public Transport createTransport(CompositeData compositData) throws IOException {
        Map parameters = new HashMap(compositData.getParameters());
        DiscoveryTransport transport = new DiscoveryTransport(createTransport(parameters));
        
        DiscoveryAgent discoveryAgent = DiscoveryAgentFactory.createDiscoveryAgent(compositData.getComponents()[0]);
        transport.setDiscoveryAgent(discoveryAgent);
        IntrospectionSupport.setProperties(transport,parameters);

        return transport;
    }

    public TransportServer doBind(String brokerId,URI location) throws IOException{
        throw new IOException("Invalid server URI: "+location);
//        try{
//            CompositeData compositData=URISupport.parseComposite(location);
//            URI[] components=compositData.getComponents();
//            if(components.length!=1){
//                throw new IOException("Invalid location: "+location
//                                +", the location must have 1 and only 1 composite URI in it - components = "
//                                +components.length);
//            }
//            Map parameters=new HashMap(compositData.getParameters());
//            DiscoveryTransportServer server=new DiscoveryTransportServer(TransportFactory.bind(brokerId,components[0]));
//            IntrospectionSupport.setProperties(server,parameters,"discovery");
//            DiscoveryAgent discoveryAgent=DiscoveryAgentFactory.createDiscoveryAgent(server.getDiscovery());
//            // Use the host name to configure the group of the discovery agent.
//            if(!parameters.containsKey("discovery.group")){
//                if(compositData.getHost()!=null){
//                    parameters.put("discovery.group",compositData.getHost());
//                }
//            }
//            if(!parameters.containsKey("discovery.brokerName")){
//                parameters.put("discovery.brokerName",brokerId);
//            }
//            IntrospectionSupport.setProperties(discoveryAgent,parameters,"discovery.");
//            server.setDiscoveryAgent(discoveryAgent);
//            return server;
//        }catch(URISyntaxException e){
//            throw new IOException("Invalid location: "+location);
//        }
    }

}