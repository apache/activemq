package org.apache.activemq.store.jdbc;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.cursors.NegativeQueueTest;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCNegativeQueueTest extends NegativeQueueTest {

    EmbeddedDataSource dataSource;
    
    protected void configureBroker(BrokerService answer) throws Exception {
        super.configureBroker(answer);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);     
        answer.setPersistenceAdapter(jdbc);
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
