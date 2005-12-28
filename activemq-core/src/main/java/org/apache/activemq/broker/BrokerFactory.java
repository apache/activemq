/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.broker;

import org.activeio.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;

import java.io.IOException;
import java.net.URI;

/**
 * A helper class to create a fully configured broker service using a URI.
 * 
 * @version $Revision$
 */
public class BrokerFactory {

    static final private FactoryFinder brokerFactoryHandlerFinder = new FactoryFinder("META-INF/services/org/apache/activemq/broker/");    

    public interface BrokerFactoryHandler {
        public BrokerService createBroker(URI brokerURI) throws Exception;
    }
    
    public static BrokerFactoryHandler createBrokerFactoryHandler(String type) throws IOException {
        try {
            return (BrokerFactoryHandler)brokerFactoryHandlerFinder.newInstance(type);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could load "+type+" factory:"+e, e);
        }
    }

    /**
     * Creates a broker from a URI configuration
     * @param brokerURI
     * @throws Exception 
     */
    public static BrokerService createBroker(URI brokerURI) throws Exception {
        if( brokerURI.getScheme() == null )
            throw new IllegalArgumentException("Invalid broker URI, no scheme specified: "+brokerURI);
        
        BrokerFactoryHandler handler = createBrokerFactoryHandler(brokerURI.getScheme());
        BrokerService broker = handler.createBroker(brokerURI);
        return broker;
    }

}
