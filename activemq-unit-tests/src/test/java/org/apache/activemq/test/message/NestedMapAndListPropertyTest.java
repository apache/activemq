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
package org.apache.activemq.test.message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsAndEmbeddedBrokerTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that a Message can have nested Map and List properties attached.
 *
 */
public class NestedMapAndListPropertyTest extends JmsTopicSendReceiveWithTwoConnectionsAndEmbeddedBrokerTest {

    private static final Logger LOG = LoggerFactory.getLogger(NestedMapAndListPropertyTest.class);

    @Override
    @SuppressWarnings("rawtypes")
    protected void assertMessageValid(int index, Message message) throws JMSException {
        Object value = message.getObjectProperty("textField");
        assertEquals("textField", data[index], value);

        Map map = (Map)message.getObjectProperty("mapField");
        assertNotNull(map);
        assertEquals("mapField.a", "foo", map.get("a"));
        assertEquals("mapField.b", Integer.valueOf(23), map.get("b"));
        assertEquals("mapField.c", Long.valueOf(45), map.get("c"));

        value = map.get("d");
        assertTrue("mapField.d should be a Map", value instanceof Map);
        map = (Map)value;

        assertEquals("mapField.d.x", "abc", map.get("x"));
        value = map.get("y");
        assertTrue("mapField.d.y is a List", value instanceof List);
        List list = (List)value;
        LOG.debug("mapField.d.y: " + list);
        assertEquals("listField.size", 3, list.size());

        LOG.debug("Found map: " + map);

        list = (List)message.getObjectProperty("listField");
        LOG.debug("listField: " + list);
        assertEquals("listField.size", 3, list.size());
        assertEquals("listField[0]", "a", list.get(0));
        assertEquals("listField[1]", "b", list.get(1));
        assertEquals("listField[2]", "c", list.get(2));
        assertEquals("JohnDoe", message.getStringProperty("JMSXUserID"));
    }

    @Override
    protected Message createMessage(int index) throws JMSException {
        Message answer = session.createMessage();

        answer.setStringProperty("textField", data[index]);

        Map<String, Object> grandChildMap = new HashMap<String, Object>();
        grandChildMap.put("x", "abc");
        grandChildMap.put("y", Arrays.asList(new Object[] {"a", "b", "c"}));

        Map<String, Object> nestedMap = new HashMap<String, Object>();
        nestedMap.put("a", "foo");
        nestedMap.put("b", Integer.valueOf(23));
        nestedMap.put("c", Long.valueOf(45));
        nestedMap.put("d", grandChildMap);

        answer.setObjectProperty("mapField", nestedMap);
        answer.setObjectProperty("listField", Arrays.asList(new Object[] {"a", "b", "c"}));
        answer.setStringProperty("JMSXUserID", "JohnDoe");

        return answer;
    }
}
