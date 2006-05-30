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
package org.apache.activemq.broker.policy;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

import javax.jms.Destination;

/**
 *
 * @version $Revision$
 */
public class IndividualDeadLetterViaXmlTest extends DeadLetterTest {


    protected BrokerService createBroker() throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/policy/individual-dlq.xml"));
        factory.afterPropertiesSet();
        BrokerService answer = factory.getBroker();
        return answer;
    }

    protected Destination createDlqDestination() {
        String queueName = "Test.DLQ." + getClass().getName() + "." + getName();
        log.info("Using queue name: " + queueName);
        return new ActiveMQQueue(queueName);
    }
}
