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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.MessageList;

/**
 * A {@link MessageList} implementation which generates a random body
 * which can help  test performance when using compression.
 *  
 * @version $Revision: 1.1 $
 */
public class RandomMessageListImpl extends MessageListImpl {
    
    public RandomMessageListImpl(int numberOfMessages, int charactersPerMessage) {
        super(numberOfMessages, charactersPerMessage);
    }

    protected Object createTextPayload(int messageCounter, int charactersPerMessage) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < charactersPerMessage; i++) {
            char ch = (char) (32 + (int) (Math.random() * 230));
            buffer.append(ch);
        }
        String answer = buffer.toString();
        assertEquals("String length should equal the requested length", charactersPerMessage, answer.length());
        return answer;
    }
}
