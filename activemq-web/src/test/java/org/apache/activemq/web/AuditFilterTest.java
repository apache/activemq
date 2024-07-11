/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class AuditFilterTest {
    private AuditFilter auditFilter;

    @Before
    public void setup() {
        auditFilter = new AuditFilter();
    }

    @Test
    public void testDoFilterRedactSensitiveData() {
        Map<String, String[]> redactedParams = auditFilter.redactSensitiveHttpParameters(new HashMap<>() {{
            put("JMSText", new String[] {"Test JMS Text"});
            put("JMSDestinationType", new String[] {"Queue"});
        }});
        assert Arrays.equals((redactedParams.get("JMSText")), new String[]{"*****"});
        assert Arrays.equals((redactedParams.get("JMSDestinationType")), new String[]{"Queue"});
    }
}