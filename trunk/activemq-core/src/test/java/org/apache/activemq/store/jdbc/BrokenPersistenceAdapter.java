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

import java.io.IOException;

import org.apache.activemq.broker.ConnectionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BrokenPersistenceAdapter extends JDBCPersistenceAdapter {

    private final Logger LOG = LoggerFactory.getLogger(BrokenPersistenceAdapter.class);

    private boolean shouldBreak = false;

    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        if ( shouldBreak ) {
            LOG.warn("Throwing exception on purpose");
            throw new IOException("Breaking on purpose");
        }
        LOG.debug("in commitTransaction");
        super.commitTransaction(context);
    }

    public void setShouldBreak(boolean shouldBreak) {
        this.shouldBreak = shouldBreak;
    }
}

