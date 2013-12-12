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
package org.apache.activemq.shiro;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.shiro.env.Environment;

/**
 * A reference (handle) to a client's {@link ConnectionContext} and {@link ConnectionInfo} as well as the Shiro
 * {@link Environment}.
 * <p/>
 * This implementation primarily exists as a <a href="http://sourcemaking.com/refactoring/introduce-parameter-object">
 * Parameter Object Design Pattern</a> implementation to eliminate long parameter lists, but provides additional
 * benefits, such as immutability and non-null guarantees, and possibility for future data without forcing method
 * signature changes.
 *
 * @since 5.10.0
 */
public class ConnectionReference {

    private final ConnectionContext connectionContext;
    private final ConnectionInfo connectionInfo;
    private final Environment environment;

    public ConnectionReference(ConnectionContext connCtx, ConnectionInfo connInfo, Environment environment) {
        if (connCtx == null) {
            throw new IllegalArgumentException("ConnectionContext argument cannot be null.");
        }
        if (connInfo == null) {
            throw new IllegalArgumentException("ConnectionInfo argument cannot be null.");
        }
        if (environment == null) {
            throw new IllegalArgumentException("Environment argument cannot be null.");
        }
        this.connectionContext = connCtx;
        this.connectionInfo = connInfo;
        this.environment = environment;
    }

    public ConnectionContext getConnectionContext() {
        return connectionContext;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public Environment getEnvironment() {
        return environment;
    }
}
