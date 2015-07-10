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
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Test;

public class StringToListOfActiveMQDestinationConverterTest {

    @Test
    public void testConvertToActiveMQDestination() {

        List<ActiveMQDestination> result = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination("");
        assertNull(result);

        result = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination("[]");
        assertNull(result);
        result = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination("[  ]");
        assertNull(result);

        result = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination("[one,two,three]");
        assertNotNull(result);
        assertEquals(3, result.size());

        result = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination("[one, two, three  ]");
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testConvertFromActiveMQDestination() {
        String result = StringToListOfActiveMQDestinationConverter.convertFromActiveMQDestination(null);
        assertNull(result);
    }
}
