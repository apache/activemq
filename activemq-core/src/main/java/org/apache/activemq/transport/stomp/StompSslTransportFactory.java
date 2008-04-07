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
package org.apache.activemq.transport.stomp;

import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.xbean.XBeanBrokerService;
import org.springframework.context.ApplicationContext;

/**
 * A <a href="http://stomp.codehaus.org/">STOMP</a> over SSL transport factory
 * 
 * @version $Revision$
 */
public class StompSslTransportFactory extends SslTransportFactory implements BrokerServiceAware {

    private ApplicationContext applicationContext = null;

    protected String getDefaultWireFormatType() {
        return "stomp";
    }

    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new StompTransportFilter(transport, new LegacyFrameTranslator(), applicationContext);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
    }

    public void setBrokerService(BrokerService brokerService) {
        if (brokerService instanceof XBeanBrokerService) {
            this.applicationContext = ((XBeanBrokerService)brokerService).getApplicationContext();
        }
    }

}
