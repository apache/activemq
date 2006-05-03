/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq;

import org.apache.activemq.test.JmsTopicSendReceiveTest;


/**
 * @version $Revision: 1.2 $
 */
public class JmsQueueSendReceiveTest extends JmsTopicSendReceiveTest {

    /**
     * Set up the test with a queue. 
     * 
     * @see junit.framework.TestCase#setUp()
     */	
    protected void setUp() throws Exception {
        topic = false;
        super.setUp();
    }
}
