package org.apache.activemq.broker.ft;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class DbRestartJDBCQueueMasterSlaveTest extends JDBCQueueMasterSlaveTest {
    private static final transient Log LOG = LogFactory.getLog(DbRestartJDBCQueueMasterSlaveTest.class);
    
    protected void messageSent() throws Exception {    
        if (++inflightMessageCount == failureCount) {
            LOG.info("STOPPING DB!@!!!!");
            final EmbeddedDataSource ds = getExistingDataSource();
            ds.setShutdownDatabase("shutdown");
            LOG.info("DB STOPPED!@!!!!");
            
            Thread dbRestartThread = new Thread("db-re-start-thread") {
                public void run() {
                    LOG.info("Waiting for master broker to Stop");
                    master.waitUntilStopped();
                    ds.setShutdownDatabase("false");
                    LOG.info("DB RESTARTED!@!!!!");
                }
            };
            dbRestartThread.start();
        }
    }
     
    protected void sendToProducer(MessageProducer producer,
            Destination producerDestination, Message message) throws JMSException {
        {   
            // do some retries as db failures filter back to the client until broker sees
            // db lock failure and shuts down
            boolean sent = false;
            do {
                try { 
                    producer.send(producerDestination, message);
                    sent = true;
                } catch (JMSException e) {
                    LOG.info("Exception on producer send:", e);
                    try { 
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }
            } while(!sent);
        }
    }
}
