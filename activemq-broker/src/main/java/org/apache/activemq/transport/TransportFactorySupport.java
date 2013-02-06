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
package org.apache.activemq.transport;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.SslContext;

import java.io.IOException;
import java.net.URI;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportFactorySupport {

    public static TransportServer bind(BrokerService brokerService, URI location) throws IOException {
        TransportFactory tf = TransportFactory.findTransportFactory(location);
        if( brokerService!=null && tf instanceof BrokerServiceAware) {
            ((BrokerServiceAware)tf).setBrokerService(brokerService);
        }
        try {
            if( brokerService!=null ) {
                SslContext.setCurrentSslContext(brokerService.getSslContext());
            }
            return tf.doBind(location);
        } finally {
            SslContext.setCurrentSslContext(null);
        }
    }

}
