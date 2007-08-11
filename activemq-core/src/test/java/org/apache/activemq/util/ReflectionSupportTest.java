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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import junit.framework.TestCase;

public class ReflectionSupportTest extends TestCase {

    public void testSetProperties() throws URISyntaxException {
        SimplePojo pojo = new SimplePojo();
        HashMap<String, String> map = new HashMap<String, String>();        
        map.put("age", "27");
        map.put("name", "Hiram");
        map.put("enabled", "true");
        map.put("uri", "test://value");
        
        IntrospectionSupport.setProperties(pojo, map);
        
        assertEquals(27, pojo.getAge());
        assertEquals("Hiram", pojo.getName());
        assertEquals(true, pojo.isEnabled());
        assertEquals(new URI("test://value"), pojo.getUri());
    }
}
