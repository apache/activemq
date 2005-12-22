/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.store.jdbc.adapter;

import org.activemq.store.jdbc.StatementProvider;

/**
 * Axion specific Adapter.
 * 
 * Axion does not seem to support ALTER statements or sub-selects.  This means:
 * - We cannot auto upgrade the schema was we roll out new versions of ActiveMQ
 * - We cannot delete durable sub messages that have be acknowledged by all consumers.
 * 
 * @version $Revision: 1.4 $
 */
public class AxionJDBCAdapter extends StreamJDBCAdapter {

    public static StatementProvider createStatementProvider() {
        DefaultStatementProvider answer = new DefaultStatementProvider() {
            public String [] getCreateSchemaStatments() {
                return new String[]{
                    "CREATE TABLE "+tablePrefix+messageTableName+"("
                           +"ID "+sequenceDataType+" NOT NULL"
                           +", CONTAINER "+containerNameDataType
                           +", MSGID_PROD "+msgIdDataType
                           +", MSGID_SEQ "+sequenceDataType
                           +", EXPIRATION "+longDataType
                           +", MSG "+(useExternalMessageReferences ? stringIdDataType : binaryDataType)
                           +", PRIMARY KEY ( ID ) )",                          
                     "CREATE INDEX "+tablePrefix+messageTableName+"_MIDX ON "+tablePrefix+messageTableName+" (MSGID_PROD,MSGID_SEQ)",
                     "CREATE INDEX "+tablePrefix+messageTableName+"_CIDX ON "+tablePrefix+messageTableName+" (CONTAINER)",                                       
                     "CREATE TABLE "+tablePrefix+durableSubAcksTableName+"("
                           +"CONTAINER "+containerNameDataType+" NOT NULL"
                           +", CLIENT_ID "+stringIdDataType+" NOT NULL"
                           +", SUB_NAME "+stringIdDataType+" NOT NULL"
                           +", SELECTOR "+stringIdDataType
                           +", LAST_ACKED_ID "+sequenceDataType
                           +", PRIMARY KEY ( CONTAINER, CLIENT_ID, SUB_NAME))",
                        
                };
            }
            
            public String getDeleteOldMessagesStatment() {
                return "DELETE FROM "+tablePrefix+messageTableName+
                    " WHERE ( EXPIRATION<>0 AND EXPIRATION<?)";
            }

        };
        answer.setLongDataType("LONG");
        return answer;
    }
    
    public AxionJDBCAdapter() {
        this(createStatementProvider());
    }

    public AxionJDBCAdapter(StatementProvider provider) {
        super(provider);        
    }
}
