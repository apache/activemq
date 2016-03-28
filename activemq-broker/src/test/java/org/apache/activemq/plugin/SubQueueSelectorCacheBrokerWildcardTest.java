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
package org.apache.activemq.plugin;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests that presence of wildcard characters is correctly identified by SubQueueSelectorCacheBroker
 */
public class SubQueueSelectorCacheBrokerWildcardTest {
    @Test
    public void testSimpleWildcardEvaluation()  {
        assertWildcard(true, "modelInstanceId = '170' AND modelClassId LIKE 'com.whatever.something.%'");
        assertWildcard(true, "JMSMessageId LIKE '%'");
        assertWildcard(false, "modelClassId = 'com.whatever.something.%'");
    }

    @Test
    public void testEscapedWildcardEvaluation() {
        assertWildcard(true, "foo LIKE '!_%' ESCAPE '!'");
        assertWildcard(false, "_foo__ LIKE '!_!%' ESCAPE '!'");
        assertWildcard(true, "_foo_ LIKE '_%' ESCAPE '.'");
        assertWildcard(true, "JMSMessageId LIKE '%' ESCAPE '.'");
        assertWildcard(false, "_foo_ LIKE '\\_\\%' ESCAPE '\\'");
    }

    @Test
    public void testNonWildard() {
        assertWildcard(false, "type = 'UPDATE_ENTITY'");
        assertWildcard(false, "a_property = 1");
        assertWildcard(false, "percentage = '100%'");
    }

    @Test
    public void testApostrophes() {
        assertWildcard(true, "quote LIKE '''In G_d We Trust'''");
        assertWildcard(true, "quote LIKE '''In Gd We Trust''' OR quote not like '''In G_d We Trust'''");
    }

    static void assertWildcard(boolean expected, String selector) {
        assertEquals("Wildcard should "+(!expected ? " NOT ":"")+" be found in "+selector, expected, SubQueueSelectorCacheBroker.hasWildcards(selector));
    }
}
