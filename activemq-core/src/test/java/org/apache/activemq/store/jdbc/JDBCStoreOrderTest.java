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

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.StoreOrderTest;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

//  https://issues.apache.org/activemq/browse/AMQ-2594
public class JDBCStoreOrderTest extends StoreOrderTest {

    private static final Log LOG = LogFactory.getLog(JDBCStoreOrderTest.class);
    
    @Override
     protected void dumpMessages() throws Exception {
        WireFormat wireFormat = new OpenWireFormat();
        java.sql.Connection conn = ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT ID, MSG FROM ACTIVEMQ_MSGS");    
        ResultSet result = statement.executeQuery();
        while(result.next()) {
            long id = result.getLong(1);
            Message message = (Message)wireFormat.unmarshal(new ByteSequence(result.getBytes(2)));
            LOG.error("id: " + id + ", message SeqId: " + message.getMessageId().getBrokerSequenceId() + ", MSG: " + message);
        }
        statement.close();
        conn.close();
    }
    
     @Override
     protected void setPersistentAdapter(BrokerService brokerService)
             throws Exception {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        brokerService.setPersistenceAdapter(jdbc);
    }

}