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
package org.apache.activemq.perf;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
/**
 * @version $Revision: 1.3 $
 */
public class KahaDurableTopicTest extends SimpleDurableTopicTest {
    
    protected BrokerService createBroker() throws Exception{
        Resource resource=new ClassPathResource( "org/apache/activemq/perf/kahaBroker.xml");
        BrokerFactoryBean factory=new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        BrokerService result=factory.getBroker();
        result.start();
        return result;
    }

    

    

}