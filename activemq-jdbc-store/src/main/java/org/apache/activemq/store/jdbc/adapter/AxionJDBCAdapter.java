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
package org.apache.activemq.store.jdbc.adapter;

import org.apache.activemq.store.jdbc.Statements;

/**
 * Axion specific Adapter.
 * 
 * Axion does not seem to support ALTER statements or sub-selects.  This means:
 * - We cannot auto upgrade the schema was we roll out new versions of ActiveMQ
 * - We cannot delete durable sub messages that have be acknowledged by all consumers.
 * 
 * @org.apache.xbean.XBean element="axionJDBCAdapter"
 * 
 */
public class AxionJDBCAdapter extends StreamJDBCAdapter {

    @Override
    public void setStatements(Statements statements) {
        
        String[] createStatements = new String[]{
            "CREATE TABLE " + statements.getFullMessageTableName() + "("
                + "ID " + statements.getSequenceDataType() + " NOT NULL"
                + ", CONTAINER " + statements.getContainerNameDataType()
                + ", MSGID_PROD " + statements.getMsgIdDataType()
                + ", MSGID_SEQ " + statements.getSequenceDataType()
                + ", EXPIRATION " + statements.getLongDataType()
                + ", MSG " + (statements.isUseExternalMessageReferences() ? statements.getStringIdDataType() : statements.getBinaryDataType())
                + ", PRIMARY KEY ( ID ) )",                          
            "CREATE INDEX " + statements.getFullMessageTableName() + "_MIDX ON " + statements.getFullMessageTableName() + " (MSGID_PROD,MSGID_SEQ)",
            "CREATE INDEX " + statements.getFullMessageTableName() + "_CIDX ON " + statements.getFullMessageTableName() + " (CONTAINER)",                                       
            "CREATE INDEX " + statements.getFullMessageTableName() + "_EIDX ON " + statements.getFullMessageTableName() + " (EXPIRATION)",                 
            "CREATE TABLE " + statements.getFullAckTableName() + "("
                + "CONTAINER " + statements.getContainerNameDataType() + " NOT NULL"
                + ", SUB_DEST " + statements.getContainerNameDataType()
                + ", CLIENT_ID " + statements.getStringIdDataType() + " NOT NULL"
                + ", SUB_NAME " + statements.getStringIdDataType() + " NOT NULL"
                + ", SELECTOR " + statements.getStringIdDataType()
                + ", LAST_ACKED_ID " + statements.getSequenceDataType()
                + ", PRIMARY KEY ( CONTAINER, CLIENT_ID, SUB_NAME))"
        };
        statements.setCreateSchemaStatements(createStatements);
        statements.setLongDataType("LONG");
        statements.setSequenceDataType("LONG");
        
        super.setStatements(statements);
    }
    
}
