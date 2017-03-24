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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.ClientFuture;
import org.apache.activemq.transport.amqp.client.util.ClientFutureSynchronization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines a context under which resources in a given session
 * will operate inside transaction scoped boundaries.
 */
public class AmqpTransactionContext {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionContext.class);

    private final AmqpSession session;
    private final Set<AmqpReceiver> txReceivers = new LinkedHashSet<AmqpReceiver>();

    private AmqpTransactionCoordinator coordinator;
    private AmqpTransactionId transactionId;

    public AmqpTransactionContext(AmqpSession session) {
        this.session = session;
    }

    /**
     * Begins a new transaction scoped to the target session.
     *
     * @param txId
     *      The transaction Id to use for this new transaction.
     *
     * @throws Exception if an error occurs while starting the transaction.
     */
    public void begin() throws Exception {
        if (transactionId != null) {
            throw new IOException("Begin called while a TX is still Active.");
        }

        final AmqpTransactionId txId = session.getConnection().getNextTransactionId();
        final ClientFuture request = new ClientFuture(new ClientFutureSynchronization() {

            @Override
            public void onPendingSuccess() {
                transactionId = txId;
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                transactionId = null;
            }
        });

        LOG.info("Attempting to Begin TX:[{}]", txId);

        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                if (coordinator == null || coordinator.isClosed()) {
                    LOG.info("Creating new Coordinator for TX:[{}]", txId);
                    coordinator = new AmqpTransactionCoordinator(session);
                    coordinator.open(new AsyncResult() {

                        @Override
                        public void onSuccess() {
                            try {
                                LOG.info("Attempting to declare TX:[{}]", txId);
                                coordinator.declare(txId, request);
                            } catch (Exception e) {
                                request.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Throwable result) {
                            request.onFailure(result);
                        }

                        @Override
                        public boolean isComplete() {
                            return request.isComplete();
                        }
                    });
                } else {
                    try {
                        LOG.info("Attempting to declare TX:[{}]", txId);
                        coordinator.declare(txId, request);
                    } catch (Exception e) {
                        request.onFailure(e);
                    }
                }

                session.pumpToProtonTransport(request);
            }
        });

        request.sync();
    }

    /**
     * Commit this transaction which then ends the lifetime of the transacted operation.
     *
     * @throws Exception if an error occurs while performing the commit
     */
    public void commit() throws Exception {
        if (transactionId == null) {
            throw new IllegalStateException("Commit called with no active Transaction.");
        }

        preCommit();

        final ClientFuture request = new ClientFuture(new ClientFutureSynchronization() {

            @Override
            public void onPendingSuccess() {
                transactionId = null;
                postCommit();
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                transactionId = null;
                postCommit();
            }
        });

        LOG.debug("Commit on TX[{}] initiated", transactionId);
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Attempting to commit TX:[{}]", transactionId);
                    coordinator.discharge(transactionId, request, true);
                    session.pumpToProtonTransport(request);
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    /**
     * Rollback any transacted work performed under the current transaction.
     *
     * @throws Exception if an error occurs during the rollback operation.
     */
    public void rollback() throws Exception {
        if (transactionId == null) {
            throw new IllegalStateException("Rollback called with no active Transaction.");
        }

        preRollback();

        final ClientFuture request = new ClientFuture(new ClientFutureSynchronization() {

            @Override
            public void onPendingSuccess() {
                transactionId = null;
                postRollback();
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                transactionId = null;
                postRollback();
            }
        });

        LOG.debug("Rollback on TX[{}] initiated", transactionId);
        session.getScheduler().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("Attempting to roll back TX:[{}]", transactionId);
                    coordinator.discharge(transactionId, request, false);
                    session.pumpToProtonTransport(request);
                } catch (Exception e) {
                    request.onFailure(e);
                }
            }
        });

        request.sync();
    }

    //----- Internal access to context properties ----------------------------//

    AmqpTransactionCoordinator getCoordinator() {
        return coordinator;
    }

    AmqpTransactionId getTransactionId() {
        return transactionId;
    }

    boolean isInTransaction() {
        return transactionId != null;
    }

    void registerTxConsumer(AmqpReceiver consumer) {
        txReceivers.add(consumer);
    }

    //----- Transaction pre / post completion --------------------------------//

    private void preCommit() {
        for (AmqpReceiver receiver : txReceivers) {
            receiver.preCommit();
        }
    }

    private void preRollback() {
        for (AmqpReceiver receiver : txReceivers) {
            receiver.preRollback();
        }
    }

    private void postCommit() {
        for (AmqpReceiver receiver : txReceivers) {
            receiver.postCommit();
        }

        txReceivers.clear();
    }

    private void postRollback() {
        for (AmqpReceiver receiver : txReceivers) {
            receiver.postRollback();
        }

        txReceivers.clear();
    }
}
