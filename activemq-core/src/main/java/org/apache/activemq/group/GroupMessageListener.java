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

package org.apache.activemq.group;


/**
 * A listener for message communication
 *
 */
public interface GroupMessageListener {
    
    /**
     * Called when a message is delivered to the <CODE>Group</CODE> from another member
     * @param sender the member who sent the message
     * @param replyId the id to use to respond to a message
     * @param message the message object
     */
    void messageDelivered(Member sender, String replyId, Object message);
}