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
package org.apache.activemq.broker.ft;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;

// prevent concurrent calls from attempting to create the db at the same time
// can result in "already exists in this jvm" errors

public class SyncCreateDataSource implements DataSource {
    final EmbeddedDataSource delegate;

    public SyncCreateDataSource(EmbeddedDataSource dataSource) {
        this.delegate = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (this) {
            return delegate.getConnection();
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        synchronized (this) {
            return delegate.getConnection();
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public EmbeddedDataSource getDelegate() {
        return delegate;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}