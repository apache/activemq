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
package org.apache.activemq.broker.store;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.store.DefaultPersistenceAdapterFactory;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.journal.JournalPersistenceAdapter;

/**
 * Once the wire format is completed we can test against real persistence storage.
 * 
 * @version $Revision$
 */
public class DefaultStoreBrokerTest extends BrokerTest {

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?deleteAllMessagesOnStartup=true"));
    }

    protected PersistenceAdapter createPersistenceAdapter(boolean clean) throws IOException {
        File dataDir = new File("test-data");
        if( clean ) {
            recursiveDelete(new File(dataDir, "journal"));
        }
        DefaultPersistenceAdapterFactory factory = new DefaultPersistenceAdapterFactory();
        factory.setDataDirectory(dataDir);
        // Use a smaller journal so that tests are quicker.
        factory.setJournalLogFileSize(1024*64);
        PersistenceAdapter adapter = factory.createPersistenceAdapter(); 
        if( clean ) {
            DataSource ds = ((JDBCPersistenceAdapter)((JournalPersistenceAdapter)adapter).getLongTermPersistence()).getDataSource();
            try {
                Connection c = ds.getConnection();
                Statement s = c.createStatement();
                try { s.executeUpdate("DROP TABLE ACTIVEMQ_MSGS");} catch (SQLException e) {}
                try { s.executeUpdate("DROP TABLE ACTIVEMQ_TXS");} catch (SQLException e) {}
                try { s.executeUpdate("DROP TABLE ACTIVEMQ_ACKS");} catch (SQLException e) {}
            } catch (SQLException e) {
            }
        }
        return adapter;
    }
    
    public static Test suite() {
        return suite(DefaultStoreBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
