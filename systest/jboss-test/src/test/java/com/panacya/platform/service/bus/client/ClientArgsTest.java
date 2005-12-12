/** 
 * 
 * Copyright 2004 Michael Gaffney
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package com.panacya.platform.service.bus.client;

import junit.framework.TestCase;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class ClientArgsTest extends TestCase {

    public ClientArgsTest(String name) {
        super(name);
    }
    
    public void testThreeArgs() {
        Long timeout = new Long(14999);
        
        String[] args = { "send", "topic.testTopic", timeout.toString()};
        ClientArgs c = new ClientArgs(args);
        assertEquals(args[0], c.getCommand());
        assertEquals(args[1], c.getDestination());
        assertEquals(timeout.longValue(), c.getTimeout());
    }

    public void testTwoArgs() {        
        String[] args = { "send", "topic.testTopic"};
        ClientArgs c = new ClientArgs(args);
        assertEquals(args[0], c.getCommand());
        assertEquals(args[1], c.getDestination());
        assertEquals(-1, c.getTimeout());
    }

}
