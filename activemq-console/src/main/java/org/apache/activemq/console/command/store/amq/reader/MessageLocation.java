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
package org.apache.activemq.console.command.store.amq.reader;

import javax.jms.Message;

import org.apache.activemq.kaha.impl.async.Location;

/**
 * A holder for a message
 *
 */
class MessageLocation {
    private Message message;
    private Location location;

    
    /**
     * @return the location
     */
    public Location getLocation() {
        return location;
    }

    /**
     * @param location
     */
    public void setLocation(Location location) {
        this.location = location;
    }
    /**
     * @return the message
     */
    public Message getMessage() {
        return message;
    }

    /**
     * @param message
     */
    public void setMessage(Message message) {
        this.message = message;
    }

    
}
