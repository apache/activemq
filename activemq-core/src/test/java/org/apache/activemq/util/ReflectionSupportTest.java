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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

public class ReflectionSupportTest extends TestCase {
	
    List<ActiveMQDestination> favorites = new ArrayList<ActiveMQDestination>();
    String favoritesString = "[queue://test, topic://test]";
    List<ActiveMQDestination> nonFavorites = new ArrayList<ActiveMQDestination>();
    String nonFavoritesString = "[topic://test1]";
    
    public void setUp() {
        favorites.add(new ActiveMQQueue("test"));
        favorites.add(new ActiveMQTopic("test"));
        nonFavorites.add(new ActiveMQTopic("test1"));
    }

    public void testSetProperties() throws URISyntaxException {
        SimplePojo pojo = new SimplePojo();
        HashMap<String, String> map = new HashMap<String, String>();        
        map.put("age", "27");
        map.put("name", "Hiram");
        map.put("enabled", "true");
        map.put("uri", "test://value");        
        map.put("favorites", favoritesString);
        map.put("nonFavorites", nonFavoritesString);
        map.put("others", null);
        
        IntrospectionSupport.setProperties(pojo, map);
        
        assertEquals(27, pojo.getAge());
        assertEquals("Hiram", pojo.getName());
        assertEquals(true, pojo.isEnabled());
        assertEquals(new URI("test://value"), pojo.getUri());
        assertEquals(favorites, pojo.getFavorites());
        assertEquals(nonFavorites, pojo.getNonFavorites());
        assertNull(pojo.getOthers());
    }
    
    public void testGetProperties() {
    	SimplePojo pojo = new SimplePojo();
    	pojo.setAge(31);
    	pojo.setName("Dejan");
    	pojo.setEnabled(true);
    	pojo.setFavorites(favorites);
    	pojo.setNonFavorites(nonFavorites);
    	pojo.setOthers(null);
    	
    	Properties props = new Properties();
    	
    	IntrospectionSupport.getProperties(pojo, props, null);
    	
    	assertEquals("Dejan", props.get("name"));
    	assertEquals("31", props.get("age"));
    	assertEquals("True", props.get("enabled"));
    	assertEquals(favoritesString, props.get("favorites"));
    	assertEquals(nonFavoritesString, props.get("nonFavorites"));
    	assertNull(props.get("others"));
    }
}
