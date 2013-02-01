package org.apache.activemq.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.wireformat.ObjectStreamWireFormat;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class JDBCConfigTest {

    protected static final String JOURNAL_ROOT = "target/test-data/";
    protected static final String DERBY_ROOT = "target/test-data/";
    protected static final String CONF_ROOT = "src/test/resources/org/apache/activemq/config/sample-conf/";
    private static final Logger LOG = LoggerFactory.getLogger(JDBCConfigTest.class);

    /*
     * This tests creating a jdbc persistence adapter using xbeans-spring
     */
    @Test
    public void testJdbcConfig() throws Exception {
        File journalFile = new File(JOURNAL_ROOT + "testJDBCConfig/journal");
        recursiveDelete(journalFile);

        File derbyFile = new File(DERBY_ROOT + "testJDBCConfig/derbydb"); // Default
        recursiveDelete(derbyFile);

        BrokerService broker;
        broker = createBroker(new FileSystemResource(CONF_ROOT + "jdbc-example.xml"));
        try {
            assertEquals("Broker Config Error (brokerName)", "brokerJdbcConfigTest", broker.getBrokerName());

            PersistenceAdapter adapter = broker.getPersistenceAdapter();

            assertTrue("Should have created a jdbc persistence adapter", adapter instanceof JDBCPersistenceAdapter);
            assertEquals("JDBC Adapter Config Error (cleanupPeriod)", 60000, ((JDBCPersistenceAdapter) adapter).getCleanupPeriod());
            assertTrue("Should have created an EmbeddedDataSource", ((JDBCPersistenceAdapter) adapter).getDataSource() instanceof EmbeddedDataSource);
            assertTrue("Should have created a DefaultWireFormat", ((JDBCPersistenceAdapter) adapter).getWireFormat() instanceof ObjectStreamWireFormat);

            LOG.info("Success");
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    protected static void recursiveDelete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        file.delete();
    }

    protected BrokerService createBroker(String resource) throws Exception {
        return createBroker(new ClassPathResource(resource));
    }

    protected BrokerService createBroker(Resource resource) throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();

        BrokerService broker = factory.getBroker();

        assertTrue("Should have a broker!", broker != null);

        // Broker is already started by default when using the XML file
        // broker.start();

        return broker;
    }
}
