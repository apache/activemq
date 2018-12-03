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

package org.apache.activemq.broker;

import javax.jms.InvalidClientIDException;
import java.util.LinkedList;
import org.apache.activemq.command.ConnectionInfo;

public class StubBroker extends EmptyBroker {
    public LinkedList<AddConnectionData> addConnectionData = new LinkedList<AddConnectionData>();
    public LinkedList<RemoveConnectionData> removeConnectionData = new LinkedList<RemoveConnectionData>();

    public class AddConnectionData {
        public final ConnectionContext connectionContext;
        public final ConnectionInfo connectionInfo;

        public AddConnectionData(ConnectionContext context, ConnectionInfo info) {
            connectionContext = context;
            connectionInfo = info;
        }
    }

    public static class RemoveConnectionData {
        public final ConnectionContext connectionContext;
        public final ConnectionInfo connectionInfo;
        public final Throwable error;

        public RemoveConnectionData(ConnectionContext context, ConnectionInfo info, Throwable error) {
            connectionContext = context;
            connectionInfo = info;
            this.error = error;
        }
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        for (AddConnectionData data : addConnectionData) {
            if (data.connectionInfo.getClientId() != null && data.connectionInfo.getClientId().equals(info.getClientId())) {
                throw new InvalidClientIDException("ClientID already exists");
            }
        }
        addConnectionData.add(new AddConnectionData(context, info));
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        removeConnectionData.add(new RemoveConnectionData(context, info, error));
    }

}
