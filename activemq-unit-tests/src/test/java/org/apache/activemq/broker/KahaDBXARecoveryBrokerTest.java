package org.apache.activemq.broker;

import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

public class KahaDBXARecoveryBrokerTest  extends XARecoveryBrokerTest {

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);

        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    public static Test suite() {
        return suite(KahaDBXARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue("test");
    }

    public void testPreparedTransactionRecoveredPurgeCommitOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        stopBroker();
        if (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
            KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
            adapter.setPurgeRecoveredXATransactionStrategy("COMMIT");
            LOG.info("Setting purgeRecoveredXATransactions to true on the KahaDBPersistenceAdapter");
        }
        broker.start();

        // Setup the consumer and try receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Since committed ... they should get delivered.
        for (int i = 0; i < 4; i++) {
            assertNotNull(receiveMessage(connection));
        }
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;

        //These should be purged so expect 0
        assertEquals(0, dar.getData().length);

    }

    public void testPreparedTransactionRecoveredPurgeRollbackOnRestart() throws Exception {

        ActiveMQDestination destination = createDestination();

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Prepare 4 message sends.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            XATransactionId txid = createXATransaction(sessionInfo);
            connection.send(createBeginTransaction(connectionInfo, txid));

            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            message.setTransactionId(txid);
            connection.send(message);

            // Prepare
            connection.send(createPrepareTransaction(connectionInfo, txid));
        }

        // Since prepared but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        stopBroker();
        if (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
            KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter)broker.getPersistenceAdapter();
            adapter.setPurgeRecoveredXATransactionStrategy("ROLLBACK");
            LOG.info("Setting purgeRecoveredXATransactions to true on the KahaDBPersistenceAdapter");
        }
        broker.start();

        // Setup the consumer and try receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Since rolledback but not committed.. they should not get delivered.
        assertNull(receiveMessage(connection));
        assertNoMessagesLeft(connection);

        Response response = connection.request(new TransactionInfo(connectionInfo.getConnectionId(), null, TransactionInfo.RECOVER));
        assertNotNull(response);
        DataArrayResponse dar = (DataArrayResponse)response;

        //These should be purged so expect 0
        assertEquals(0, dar.getData().length);

    }
}