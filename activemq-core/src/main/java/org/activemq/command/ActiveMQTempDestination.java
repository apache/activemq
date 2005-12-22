/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.command;

import javax.jms.JMSException;

import org.activemq.ActiveMQConnection;

/**
 * @openwire:marshaller
 * @version $Revision: 1.5 $
 */
abstract public class ActiveMQTempDestination extends ActiveMQDestination {
    
    protected transient ActiveMQConnection connection;
    protected transient String connectionId;
    protected transient int sequenceId;
    
    public ActiveMQTempDestination() {
    }
    
    public ActiveMQTempDestination(String name) {
        super(name);
    }
    
    public ActiveMQTempDestination(String connectionId, long sequenceId) {
        super(connectionId+":"+sequenceId);
    }

    public boolean isTemporary() {
        return true;
    }

    public void delete() throws JMSException {
        connection.deleteTempDestination(this);
    }

    public ActiveMQConnection getConnection() {
        return connection;
    }

    public void setConnection(ActiveMQConnection connection) {
        this.connection = connection;
    }

    public void setPhysicalName(String physicalName) {
        super.setPhysicalName(physicalName);
        if( !isComposite() ) {
            // Parse off the sequenceId off the end.
            int p = this.physicalName.lastIndexOf(":");
            sequenceId = Integer.parseInt(this.physicalName.substring(p+1));
            // The rest should be the connection id.
            connectionId = this.physicalName.substring(0,p);
        }
    }

    public String getConnectionId() {
        return connectionId;
    }

    public int getSequenceId() {
        return sequenceId;
    }
}
