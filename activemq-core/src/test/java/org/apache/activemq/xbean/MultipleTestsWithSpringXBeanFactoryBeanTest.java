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
package org.apache.activemq.xbean;

import org.apache.activemq.broker.BrokerService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @version $Revision$
 */
public class MultipleTestsWithSpringXBeanFactoryBeanTest extends MultipleTestsWithEmbeddedBrokerTest {

    private ClassPathXmlApplicationContext context;

    protected BrokerService createBroker() throws Exception {
        context = new ClassPathXmlApplicationContext("org/apache/activemq/xbean/spring2.xml");
        return  (BrokerService) context.getBean("broker");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (context != null) {
            context.destroy();
        }
    }

    

}

