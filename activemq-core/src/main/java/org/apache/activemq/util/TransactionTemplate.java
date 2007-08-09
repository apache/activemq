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
package org.apache.activemq.util;

import java.io.IOException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A helper class for running code with a PersistenceAdapter in a transaction.
 * 
 * @version $Revision: 1.2 $
 */
public class TransactionTemplate {
    private static final Log LOG = LogFactory.getLog(TransactionTemplate.class);
    private PersistenceAdapter persistenceAdapter;
    private ConnectionContext context;

    public TransactionTemplate(PersistenceAdapter persistenceAdapter, ConnectionContext context) {
        this.persistenceAdapter = persistenceAdapter;
        this.context = context;
    }

    public void run(Callback task) throws IOException {
        persistenceAdapter.beginTransaction(context);
        Throwable throwable = null;
        try {
            task.execute();
        } catch (IOException t) {
            throwable = t;
            throw t;
        } catch (RuntimeException t) {
            throwable = t;
            throw t;
        } catch (Throwable t) {
            throwable = t;
            throw IOExceptionSupport.create("Persistence task failed: " + t, t);
        } finally {
            if (throwable == null) {
                persistenceAdapter.commitTransaction(context);
            } else {
                LOG.error("Having to Rollback - caught an exception: " + throwable);
                persistenceAdapter.rollbackTransaction(context);
            }
        }
    }

    public ConnectionContext getContext() {
        return context;
    }

    public PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }
}
