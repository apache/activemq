/*
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
package org.apache.activemq;

import static org.apache.activemq.ActiveMQConnectionMetaData.PLATFORM_DETAILS_MAX_LENGTH;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ActiveMQConnectionMetaDataTest {

    @Test
    public void testPlatformDetails() {
        // static final should match generated from the utility method
        assertEquals(ActiveMQConnectionMetaData.PLATFORM_DETAILS,
                ActiveMQConnectionMetaData.getPlatformDetails());
    }

    @Test
    public void testPlatformDetailsTruncation() {
        String javaVendor = System.getProperty("java.vendor");
        try {
            System.setProperty("java.vendor", "a".repeat(PLATFORM_DETAILS_MAX_LENGTH + 1));
            // ensure we truncate if too large
            assertEquals(PLATFORM_DETAILS_MAX_LENGTH,
                    ActiveMQConnectionMetaData.getPlatformDetails().length());
        } finally {
            // restore original
            System.setProperty("java.vendor", javaVendor);
        }
    }
}
