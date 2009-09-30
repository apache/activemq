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
package org.apache.activemq.store.amq.reader;

import java.util.Iterator;

import javax.jms.Message;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * An Iterator for the AMQReader
 *
 */
class AMQIterator  implements Iterator<Message>{
    private AMQReader reader;
    private BooleanExpression expression;
    private MessageLocation currentLocation;
    private MessageLocation nextLocation;
    private boolean valid=true;
    
        
    AMQIterator(AMQReader reader, BooleanExpression expression){
        this.reader=reader;
        this.expression=expression;
    } 
    
    public boolean hasNext() {
        try {
            this.nextLocation = reader.getNextMessage(currentLocation);
            Message next = nextLocation != null ? nextLocation.getMessage()
                    : null;
            if (expression == null) {
                return next != null;
            } else {
                while (next != null) {
                    MessageEvaluationContext context = new MessageEvaluationContext();
                    context.setMessageReference((MessageReference) next);
                    if (expression.matches(context)) {
                        return true;
                    }
                    this.nextLocation = reader.getNextMessage(currentLocation);
                    next = nextLocation != null ? nextLocation.getMessage()
                            : null;
                }
                valid=false;
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to get next message from reader ", e);
        }
    }

   
    public Message next() {
        if (valid && (nextLocation != null || hasNext())) {
            this.currentLocation=nextLocation;
            return nextLocation.getMessage();
        }
        return null;
    }

   
    public void remove() {
        throw new IllegalStateException("Not supported");
        
    }

}
