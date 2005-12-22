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
package org.activemq.store.jdbc;

/**
 * Generates the SQL statements that are used by the JDBCAdapter.
 * 
 * @version $Revision: 1.4 $
 */
public interface StatementProvider {
    
    public String[] getCreateSchemaStatments();
    public String[] getDropSchemaStatments();
    public String getAddMessageStatment();
    public String getUpdateMessageStatment();
    public String getRemoveMessageStatment();
    public String getFindMessageSequenceIdStatment();
    public String getFindMessageStatment();
    public String getFindAllMessagesStatment();
    public String getFindLastSequenceIdInMsgs();
    public String getFindLastSequenceIdInAcks();
    public String getCreateDurableSubStatment();
    public String getFindDurableSubStatment();
    public String getUpdateLastAckOfDurableSub();
    public String getFindAllDurableSubMessagesStatment();
    public String getRemoveAllMessagesStatment();
    public String getRemoveAllSubscriptionsStatment();
    public String getDeleteSubscriptionStatment();
    public String getDeleteOldMessagesStatment();
    public String getFindAllDestinationsStatment();

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences);
    public boolean isUseExternalMessageReferences();

}