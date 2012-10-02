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

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import junit.framework.TestCase;

public class DestinationFilterTest extends TestCase {

	public void testPrefixFilter() throws Exception {
		DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(">"));
		assertTrue("Filter not parsed well: " + filter.getClass(), filter instanceof PrefixDestinationFilter);
		System.out.println(filter);
		assertFalse("Filter matched wrong destination type", filter.matches(new ActiveMQTopic(">")));
	}

	public void testWildcardFilter() throws Exception {
		DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue("A.*"));
		assertTrue("Filter not parsed well: " + filter.getClass(), filter instanceof WildcardDestinationFilter);
		assertFalse("Filter matched wrong destination type", filter.matches(new ActiveMQTopic("A.B")));
	}

	public void testCompositeFilter() throws Exception {
		DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue("A.B,B.C"));
		assertTrue("Filter not parsed well: " + filter.getClass(), filter instanceof CompositeDestinationFilter);
		assertFalse("Filter matched wrong destination type", filter.matches(new ActiveMQTopic("A.B")));
	}
}
