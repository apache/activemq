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
package org.apache.activemq.filter;

import java.util.Set;

import org.apache.activemq.SpringTestSupport;
import org.apache.activemq.command.ActiveMQTopic;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @version $Revision: 1.1 $
 */
public class DummyPolicyTest extends SpringTestSupport {

    public void testPolicy() throws Exception {
        DummyPolicy policy = (DummyPolicy)getBean("policy");

        Set set = policy.get(new ActiveMQTopic("FOO.BAR"));

        assertSetEquals("FOO.BAR set", new Object[] {"Edam", "Cheddar"}, set);
    }

    protected AbstractApplicationContext createApplicationContext() {
        // TODO Auto-generated method stub
        return new ClassPathXmlApplicationContext("org/apache/activemq/filter/dummyPolicy.xml");
    }

}
