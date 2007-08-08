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
package org.apache.activemq.memory.buffer;


/**
 *
 * @version $Revision: 1.1 $
 */
public class OrderBasedMemoryBufferTest extends MemoryBufferTestSupport {

    public void testSizeWorks() throws Exception {
        qA.add(createMessage(10));
        qB.add(createMessage(10));
        qB.add(createMessage(10));
        qC.add(createMessage(10));
        
        dump();
        
        assertEquals("buffer size", 40, buffer.getSize());
        assertEquals("qA", 10, qA.getSize());
        assertEquals("qB", 20, qB.getSize());
        assertEquals("qC", 10, qC.getSize());
        
        qC.add(createMessage(10));
        
        dump();
        
        assertEquals("buffer size", 40, buffer.getSize());
        assertEquals("qA", 0, qA.getSize());
        assertEquals("qB", 20, qB.getSize());
        assertEquals("qC", 20, qC.getSize());

        qB.add(createMessage(10));
        
        dump();
        
        assertEquals("buffer size", 40, buffer.getSize());
        assertEquals("qA", 0, qA.getSize());
        assertEquals("qB", 20, qB.getSize());
        assertEquals("qC", 20, qC.getSize());

        qA.add(createMessage(10));

        dump();
        
        assertEquals("buffer size", 40, buffer.getSize());
        assertEquals("qA", 10, qA.getSize());
        assertEquals("qB", 10, qB.getSize());
        assertEquals("qC", 20, qC.getSize());
    }

    
    protected MessageBuffer createMessageBuffer() {
        return new OrderBasedMessageBuffer(40);
    }
}
