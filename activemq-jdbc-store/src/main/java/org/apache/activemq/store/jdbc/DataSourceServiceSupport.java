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
package org.apache.activemq.store.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 * A helper class which provides a factory method to create a default
 * {@link DataSource) if one is not provided.
 * 
 * 
 */
abstract public class DataSourceServiceSupport extends LockableServiceSupport {

    private String dataDirectory = IOHelper.getDefaultDataDirectory();
    private File dataDirectoryFile;
    private DataSource dataSource;
    private DataSource createdDefaultDataSource;

    public DataSourceServiceSupport() {
    }

    public DataSourceServiceSupport(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public File getDataDirectoryFile() {
        if (dataDirectoryFile == null) {
            dataDirectoryFile = new File(getDataDirectory());
        }
        return dataDirectoryFile;
    }

    public void setDataDirectoryFile(File dataDirectory) {
        this.dataDirectoryFile = dataDirectory;
    }

    public String getDataDirectory() {
        return dataDirectory;
    }

    public void setDataDirectory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public DataSource getDataSource() throws IOException {
        if (dataSource == null) {
            dataSource = createDataSource(getDataDirectoryFile().getCanonicalPath());
            if (dataSource == null) {
                throw new IllegalArgumentException("No dataSource property has been configured");
            } else {
                createdDefaultDataSource = dataSource;
            }
        }
        return dataSource;
    }

    public void closeDataSource(DataSource dataSource) {
        if (createdDefaultDataSource != null && createdDefaultDataSource.equals(dataSource)) {
            shutdownDefaultDataSource(dataSource);
            createdDefaultDataSource = this.dataSource = null;
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static DataSource createDataSource(String homeDir) throws IOException {
        return createDataSource(homeDir, "derbydb");
    }

    public static DataSource createDataSource(String homeDir, String dbName) throws IOException {

        // Setup the Derby datasource.
        System.setProperty("derby.system.home", homeDir);
        System.setProperty("derby.storage.fileSyncTransactionLog", "true");
        System.setProperty("derby.storage.pageCacheSize", "100");

        final EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName(dbName);
        ds.setCreateDatabase("create");
        return ds;
    }

    public static void shutdownDefaultDataSource(DataSource dataSource) {
        final EmbeddedDataSource ds =  (EmbeddedDataSource) dataSource;
        ds.setCreateDatabase("shutdown");
        ds.setShutdownDatabase("shutdown");
        try {
            ds.getConnection();
        } catch (SQLException expectedAndIgnored) {
        }
    }

    public String toString() {
        return "" + dataSource;
    }



}
