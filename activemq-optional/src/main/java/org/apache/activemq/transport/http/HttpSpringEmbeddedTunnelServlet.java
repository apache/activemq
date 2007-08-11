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
package org.apache.activemq.transport.http;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;


/**
 * This servlet embeds an ActiveMQ broker inside a servlet engine which is
 * ideal for deploying ActiveMQ inside a WAR and using this servlet as a HTTP tunnel.
 *
 * @version $Revision$
 */
public class HttpSpringEmbeddedTunnelServlet extends HttpEmbeddedTunnelServlet {

	private static final long serialVersionUID = -6568661997192814908L;

	/**
     * Factory method to create a new broker
     */
    protected BrokerService createBroker() throws Exception {
    	
        String configFile = getServletContext().getInitParameter("org.activemq.config.file");
        if (configFile == null) {
        	configFile="activemq.xml";
        }

        BrokerFactoryBean factory = new BrokerFactoryBean(new ClassPathResource(configFile));
        factory.afterPropertiesSet();

        return factory.getBroker();
    }

}
