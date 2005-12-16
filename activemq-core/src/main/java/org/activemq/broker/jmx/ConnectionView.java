/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.broker.jmx;

import org.activemq.broker.Connection;

public class ConnectionView implements ConnectionViewMBean {

    private final Connection connection;

    public ConnectionView(Connection connection) {
        this.connection = connection;
    }

    public void start() throws Exception {
        connection.start();
    }

    public void stop() throws Exception {
        connection.stop();
    }
    
    /**
     * @return true if the Connection is slow
     */
    public boolean isSlow() {
        return connection.isSlow();
    }
    
    /**
     * @return if after being marked, the Connection is still writing
     */
    public boolean isBlocked() {
        return connection.isBlocked();
    }
    
    
    /**
     * @return true if the Connection is connected
     */
    public boolean isConnected() {
        return connection.isConnected();
    }
    
    /**
     * @return true if the Connection is active
     */
    public boolean isActive() {
        return connection.isActive();
    }
    

    /**
     * Returns the number of messages to be dispatched to this connection
     */
    public int getDispatchQueueSize() {
        return connection.getDispatchQueueSize();
    }
}
