/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.protocolbuffer;

import junit.framework.TestCase;

/**
 * @version $Revision: 1.1 $
 */
public class TestSupport extends TestCase {
    // TODO seems like 4m messages cause Protocol Buffers to barf but 3m is fine :)
    protected long messageCount = 4 * 1000 * 1000;
    protected boolean verbose = false;
    protected boolean doAssertions = true;
    protected boolean useProducerId = false;

    protected StopWatch createStopWatch(String name) {
        StopWatch answer = new StopWatch(name);
        answer.setLogFrequency((int) messageCount / 10);
        return answer;
    }
}
