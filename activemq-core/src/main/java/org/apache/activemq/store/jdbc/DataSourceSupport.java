/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.store.jdbc;

import org.apache.derby.jdbc.EmbeddedDataSource;

import javax.sql.DataSource;

import java.io.File;
import java.io.IOException;

/**
 * A helper class which provides a factory method to create a default
 * {@link DataSource) if one is not provided.
 * 
 * @version $Revision$
 */
public class DataSourceSupport {

    private File dataDirectory;
    private DataSource dataSource;

    public DataSourceSupport() {
    }

    public DataSourceSupport(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public File getDataDirectory() {
        if (dataDirectory == null) {
            dataDirectory = new File("activemq-data");
        }
        return dataDirectory;
    }

    public void setDataDirectory(File dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    public DataSource getDataSource() throws IOException {
        if (dataSource == null) {
            dataSource = createDataSource();
            if (dataSource == null) { 
                throw new IllegalArgumentException("No dataSource property has been configured");
            }
        }
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected DataSource createDataSource() throws IOException {

        // Setup the Derby datasource.
        System.setProperty("derby.system.home", getDataDirectory().getCanonicalPath());
        System.setProperty("derby.storage.fileSyncTransactionLog", "true");
        System.setProperty("derby.storage.pageCacheSize", "100");

        final EmbeddedDataSource ds = new EmbeddedDataSource();
        ds.setDatabaseName("derbydb");
        ds.setCreateDatabase("create");
        return ds;
    }

}
