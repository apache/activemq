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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DestinationFilterTest extends TestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(DestinationFilterTest.class);

    public void testSuffixFilter() throws Exception {
        DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue("<"));
        assertTrue("Filter not parsed into wrong type: " + filter.getClass(), filter instanceof SuffixDestinationFilter);
        assertFalse("Filter matched wrong destination type", filter.matches(new ActiveMQTopic("A")));
        assertTrue("Filter is not wildcard", filter.isWildcard());
        assertMatches("<", "A");
        assertMatches("<", "A.B");
        assertMatches("<.B", "A.B");
        assertNotMatches("<.B", "A.B.C");
        assertMatches("<.<.B", "B");
        assertMatches("<.<.B", "A.B");
        assertNotMatches("<.<.B", "A.B.C");
        assertMatches("<.B.C", "A.B.C");

        assertMatches("<.B.C", "*.C");
        assertMatches("<.*.C", "B.C");
    }

    private void assertMatches(String pattern, String name) {
        DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(pattern));
        LOGGER.info("Checking name {} against filter {}...", name, filter);
        assertTrue("Filter '" + pattern + "' did not match '" + name + "'.", filter.matches(new ActiveMQQueue(name)));
    }

    private void assertNotMatches(String pattern, String name) {
        DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(pattern));
        LOGGER.info("Checking name {} against filter {}...", name, filter);
        assertFalse("Filter '" + pattern + "' matched '" + name + "'.", filter.matches(new ActiveMQQueue(name)));
    }

	public void testPrefixFilter() throws Exception {
		DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(">"));
		assertTrue("Filter not parsed well: " + filter.getClass(), filter instanceof PrefixDestinationFilter);
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

    public void testMatchesChild() throws Exception{
        DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue("A.*.C"));
        assertFalse("Filter matched wrong destination type", filter.matches(new ActiveMQTopic("A.B")));
        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A.B.C")));

        filter = DestinationFilter.parseFilter(new ActiveMQQueue("A.*"));
        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A.B")));
        assertFalse("Filter did match", filter.matches(new ActiveMQQueue("A")));
    }

    public void testMatchesAny() throws Exception{
        DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue("A.>.>"));

        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A.C")));

        assertFalse("Filter did match", filter.matches(new ActiveMQQueue("B")));
        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A.B")));
        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A.B.C.D.E.F")));
        assertTrue("Filter did not match", filter.matches(new ActiveMQQueue("A")));
    }
}
