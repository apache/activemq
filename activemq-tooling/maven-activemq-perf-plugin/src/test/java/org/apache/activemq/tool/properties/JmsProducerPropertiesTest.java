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
package org.apache.activemq.tool.properties;

import junit.framework.TestCase;

import java.util.Set;

public class JmsProducerPropertiesTest extends TestCase {
	
	/**
	 * Tests the correct parsing of message headers.
	 * @See JmsProducerProperties.setHeader(String encodedHeader)
	 * 
	 */
    public void testMessageHeaders() {
    	
    	// first test correct header values
    	String header = "a=b";
    	JmsProducerProperties props = new JmsProducerProperties();
    	props.setHeader(header);
    	assertEquals(1, props.headerMap.size());
    	Set<String> keys = props.getHeaderKeys();
    	assertEquals(1, keys.size());
    	assertTrue(keys.contains("a"));
    	assertEquals("b", props.getHeaderValue("a"));
    	props.clearHeaders();
    	
    	header = "a=b:c=d";
    	props.setHeader(header);
    	assertEquals(2, props.headerMap.size());
    	keys = props.getHeaderKeys();
    	assertEquals(2, keys.size());
    	assertTrue(keys.contains("a"));
    	assertTrue(keys.contains("c"));
    	assertEquals("b", props.getHeaderValue("a"));
    	assertEquals("d", props.getHeaderValue("c"));
    	props.clearHeaders();
    	
    	header = "a=b:c=d:e=f";
    	props.setHeader(header);
    	assertEquals(3, props.headerMap.size());
    	keys = props.getHeaderKeys();
    	assertEquals(3, keys.size());
    	assertTrue(keys.contains("a"));
    	assertTrue(keys.contains("c"));
    	assertTrue(keys.contains("e"));
    	assertEquals("b", props.getHeaderValue("a"));
    	assertEquals("d", props.getHeaderValue("c"));
    	assertEquals("f", props.getHeaderValue("e"));
    	props.clearHeaders();

    	header = "a=b:c=d:e=f:";
    	props.setHeader(header);
    	assertEquals(3, props.headerMap.size());
    	keys = props.getHeaderKeys();
    	assertEquals(3, keys.size());
    	assertTrue(keys.contains("a"));
    	assertTrue(keys.contains("c"));
    	assertTrue(keys.contains("e"));
    	assertEquals("b", props.getHeaderValue("a"));
    	assertEquals("d", props.getHeaderValue("c"));
    	assertEquals("f", props.getHeaderValue("e"));
    	props.clearHeaders();

    	
    	// test incorrect header values
    	header = "a:=";
    	props.setHeader(header);
    	assertEquals(0, props.headerMap.size());
    	props.clearHeaders();
    	
    	header = "a:=b";
    	props.setHeader(header);
    	assertEquals(0, props.headerMap.size());
    	props.clearHeaders();
    	
    	header = "a=:";
    	props.setHeader(header);
    	assertEquals(0, props.headerMap.size());
    	props.clearHeaders();
    	
    	header = "a=b::";
    	props.setHeader(header);
    	assertEquals(1, props.headerMap.size());
    	keys = props.getHeaderKeys();
    	assertEquals(1, keys.size());
    	assertTrue(keys.contains("a"));
    	assertEquals("b", props.getHeaderValue("a"));
    	props.clearHeaders();
    	
    	header = "a=b:\":";
    	props.setHeader(header);
    	assertEquals(1, props.headerMap.size());
    	keys = props.getHeaderKeys();
    	assertEquals(1, keys.size());
    	assertTrue(keys.contains("a"));
    	assertEquals("b", props.getHeaderValue("a"));
    	props.clearHeaders();
    	
    	header = " :  ";
    	props.setHeader(header);
    	assertEquals(0, props.headerMap.size());
    	props.clearHeaders();
    }
}