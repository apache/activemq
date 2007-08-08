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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

/**
 *
 * @version $Revision: 1.1 $
 */
public class BadDummyPolicyConfigTest extends TestCase {

    protected static final Log log = LogFactory.getLog(BadDummyPolicyConfigTest.class);
    protected DummyPolicy policy = new DummyPolicy();
    
    public void testNoDestinationSpecified() throws Exception {
        DummyPolicyEntry entry = new DummyPolicyEntry();
        entry.setDescription("cheese");
        
        assertFailsToSetEntries(entry);
    }
    
    public void testNoValueSpecified() throws Exception {
        DummyPolicyEntry entry = new DummyPolicyEntry();
        entry.setTopic("FOO.BAR");
        
        assertFailsToSetEntries(entry);
    }
    
    public void testValidEntry() throws Exception {
        DummyPolicyEntry entry = new DummyPolicyEntry();
        entry.setDescription("cheese");
        entry.setTopic("FOO.BAR");
        
        entry.afterPropertiesSet();
    }

    protected void assertFailsToSetEntries(DummyPolicyEntry entry) throws Exception {
        try {
            entry.afterPropertiesSet();
        }
        catch (IllegalArgumentException e) {
            log.info("Worked! Caught expected exception: " + e);
        }
    }
}
