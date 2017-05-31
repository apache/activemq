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

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.cursors.NegativeQueueTest;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCNegativeQueueTest extends NegativeQueueTest {

    DataSource dataSource;
    
    protected void configureBroker(BrokerService answer) throws Exception {
        super.configureBroker(answer);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        answer.setPersistenceAdapter(jdbc);
        dataSource = jdbc.getDataSource();
    }

    protected void tearDown() throws Exception {
        if (DEBUG) {
            printQuery("Select * from ACTIVEMQ_MSGS", System.out);
        }
        super.tearDown();
    }
    
    
    private void printQuery(String query, PrintStream out)
            throws SQLException {
        Connection conn = dataSource.getConnection();
        printQuery(conn.prepareStatement(query), out);
        conn.close();
    }

    private void printQuery(PreparedStatement s, PrintStream out)
            throws SQLException {

        ResultSet set = null;
        try {
            set = s.executeQuery();
            ResultSetMetaData metaData = set.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (i == 1)
                    out.print("||");
                out.print(metaData.getColumnName(i) + "||");
            }
            out.println();
            while (set.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    if (i == 1)
                        out.print("|");
                    out.print(set.getString(i) + "|");
                }
                out.println();
            }
        } finally {
            try {
                set.close();
            } catch (Throwable ignore) {
            }
            try {
                s.close();
            } catch (Throwable ignore) {
            }
        }
    }
}
