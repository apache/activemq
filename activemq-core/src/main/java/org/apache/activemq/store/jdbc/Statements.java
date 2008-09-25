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

/**
 * @version $Revision: 1.4 $
 * 
 * @org.apache.xbean.XBean element="statements"
 * 
 */
public class Statements {

    protected String messageTableName = "ACTIVEMQ_MSGS";
    protected String durableSubAcksTableName = "ACTIVEMQ_ACKS";
    protected String lockTableName = "ACTIVEMQ_LOCK";
    protected String binaryDataType = "BLOB";
    protected String containerNameDataType = "VARCHAR(250)";
    protected String msgIdDataType = "VARCHAR(250)";
    protected String sequenceDataType = "INTEGER";
    protected String longDataType = "BIGINT";
    protected String stringIdDataType = "VARCHAR(250)";
    protected boolean useExternalMessageReferences;

    private String tablePrefix = "";
    private String addMessageStatement;
    private String updateMessageStatement;
    private String removeMessageStatment;
    private String findMessageSequenceIdStatement;
    private String findMessageStatement;
    private String findAllMessagesStatement;
    private String findLastSequenceIdInMsgsStatement;
    private String findLastSequenceIdInAcksStatement;
    private String createDurableSubStatement;
    private String findDurableSubStatement;
    private String findAllDurableSubsStatement;
    private String updateLastAckOfDurableSubStatement;
    private String deleteSubscriptionStatement;
    private String findAllDurableSubMessagesStatement;
    private String findDurableSubMessagesStatement;
    private String findAllDestinationsStatement;
    private String removeAllMessagesStatement;
    private String removeAllSubscriptionsStatement;
    private String deleteOldMessagesStatement;
    private String[] createSchemaStatements;
    private String[] dropSchemaStatements;
    private String lockCreateStatement;
    private String lockUpdateStatement;
    private String nextDurableSubscriberMessageStatement;
    private String durableSubscriberMessageCountStatement;
    private String lastAckedDurableSubscriberMessageStatement;
    private String destinationMessageCountStatement;
    private String findNextMessagesStatement;
    private boolean useLockCreateWhereClause;

    public String[] getCreateSchemaStatements() {
        if (createSchemaStatements == null) {
            createSchemaStatements = new String[] {
                "CREATE TABLE " + getFullMessageTableName() + "(" + "ID " + sequenceDataType + " NOT NULL"
                    + ", CONTAINER " + containerNameDataType + ", MSGID_PROD " + msgIdDataType + ", MSGID_SEQ "
                    + sequenceDataType + ", EXPIRATION " + longDataType + ", MSG "
                    + (useExternalMessageReferences ? stringIdDataType : binaryDataType)
                    + ", PRIMARY KEY ( ID ) )",
                "CREATE INDEX " + getFullMessageTableName() + "_MIDX ON " + getFullMessageTableName() + " (MSGID_PROD,MSGID_SEQ)",
                "CREATE INDEX " + getFullMessageTableName() + "_CIDX ON " + getFullMessageTableName() + " (CONTAINER)",
                "CREATE INDEX " + getFullMessageTableName() + "_EIDX ON " + getFullMessageTableName() + " (EXPIRATION)",
                "CREATE TABLE " + getFullAckTableName() + "(" + "CONTAINER " + containerNameDataType + " NOT NULL"
                    + ", SUB_DEST " + stringIdDataType 
                    + ", CLIENT_ID " + stringIdDataType + " NOT NULL" + ", SUB_NAME " + stringIdDataType
                    + " NOT NULL" + ", SELECTOR " + stringIdDataType + ", LAST_ACKED_ID " + sequenceDataType
                    + ", PRIMARY KEY ( CONTAINER, CLIENT_ID, SUB_NAME))", 
                "CREATE TABLE " + getFullLockTableName() 
                    + "( ID " + longDataType + " NOT NULL, TIME " + longDataType 
                    + ", BROKER_NAME " + stringIdDataType + ", PRIMARY KEY (ID) )",
                "INSERT INTO " + getFullLockTableName() + "(ID) VALUES (1)", 
            };
        }
        return createSchemaStatements;
    }

    public String[] getDropSchemaStatements() {
        if (dropSchemaStatements == null) {
            dropSchemaStatements = new String[] {"DROP TABLE " + getFullAckTableName() + "",
                                                 "DROP TABLE " + getFullMessageTableName() + "",
                                                 "DROP TABLE " + getFullLockTableName() + ""};
        }
        return dropSchemaStatements;
    }

    public String getAddMessageStatement() {
        if (addMessageStatement == null) {
            addMessageStatement = "INSERT INTO "
                                  + getFullMessageTableName()
                                  + "(ID, MSGID_PROD, MSGID_SEQ, CONTAINER, EXPIRATION, MSG) VALUES (?, ?, ?, ?, ?, ?)";
        }
        return addMessageStatement;
    }

    public String getUpdateMessageStatement() {
        if (updateMessageStatement == null) {
            updateMessageStatement = "UPDATE " + getFullMessageTableName() + " SET MSG=? WHERE ID=?";
        }
        return updateMessageStatement;
    }

    public String getRemoveMessageStatment() {
        if (removeMessageStatment == null) {
            removeMessageStatment = "DELETE FROM " + getFullMessageTableName() + " WHERE ID=?";
        }
        return removeMessageStatment;
    }

    public String getFindMessageSequenceIdStatement() {
        if (findMessageSequenceIdStatement == null) {
            findMessageSequenceIdStatement = "SELECT ID FROM " + getFullMessageTableName()
                                             + " WHERE MSGID_PROD=? AND MSGID_SEQ=?";
        }
        return findMessageSequenceIdStatement;
    }

    public String getFindMessageStatement() {
        if (findMessageStatement == null) {
            findMessageStatement = "SELECT MSG FROM " + getFullMessageTableName() + " WHERE ID=?";
        }
        return findMessageStatement;
    }

    public String getFindAllMessagesStatement() {
        if (findAllMessagesStatement == null) {
            findAllMessagesStatement = "SELECT ID, MSG FROM " + getFullMessageTableName()
                                       + " WHERE CONTAINER=? ORDER BY ID";
        }
        return findAllMessagesStatement;
    }

    public String getFindLastSequenceIdInMsgsStatement() {
        if (findLastSequenceIdInMsgsStatement == null) {
            findLastSequenceIdInMsgsStatement = "SELECT MAX(ID) FROM " + getFullMessageTableName();
        }
        return findLastSequenceIdInMsgsStatement;
    }

    public String getFindLastSequenceIdInAcksStatement() {
        if (findLastSequenceIdInAcksStatement == null) {
            findLastSequenceIdInAcksStatement = "SELECT MAX(LAST_ACKED_ID) FROM " + getFullAckTableName();
        }
        return findLastSequenceIdInAcksStatement;
    }

    public String getCreateDurableSubStatement() {
        if (createDurableSubStatement == null) {
            createDurableSubStatement = "INSERT INTO "
                                        + getFullAckTableName()
                                        + "(CONTAINER, CLIENT_ID, SUB_NAME, SELECTOR, LAST_ACKED_ID, SUB_DEST) "
                                        + "VALUES (?, ?, ?, ?, ?, ?)";
        }
        return createDurableSubStatement;
    }

    public String getFindDurableSubStatement() {
        if (findDurableSubStatement == null) {
            findDurableSubStatement = "SELECT SELECTOR, SUB_DEST " + "FROM " + getFullAckTableName()
                                      + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return findDurableSubStatement;
    }

    public String getFindAllDurableSubsStatement() {
        if (findAllDurableSubsStatement == null) {
            findAllDurableSubsStatement = "SELECT SELECTOR, SUB_NAME, CLIENT_ID, SUB_DEST" + " FROM "
                                          + getFullAckTableName() + " WHERE CONTAINER=?";
        }
        return findAllDurableSubsStatement;
    }

    public String getUpdateLastAckOfDurableSubStatement() {
        if (updateLastAckOfDurableSubStatement == null) {
            updateLastAckOfDurableSubStatement = "UPDATE " + getFullAckTableName() + " SET LAST_ACKED_ID=?"
                                                 + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return updateLastAckOfDurableSubStatement;
    }

    public String getDeleteSubscriptionStatement() {
        if (deleteSubscriptionStatement == null) {
            deleteSubscriptionStatement = "DELETE FROM " + getFullAckTableName()
                                          + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return deleteSubscriptionStatement;
    }

    public String getFindAllDurableSubMessagesStatement() {
        if (findAllDurableSubMessagesStatement == null) {
            findAllDurableSubMessagesStatement = "SELECT M.ID, M.MSG FROM " + getFullMessageTableName()
                                                 + " M, " + getFullAckTableName() + " D "
                                                 + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                                 + " AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID"
                                                 + " ORDER BY M.ID";
        }
        return findAllDurableSubMessagesStatement;
    }

    public String getFindDurableSubMessagesStatement() {
        if (findDurableSubMessagesStatement == null) {
            findDurableSubMessagesStatement = "SELECT M.ID, M.MSG FROM " + getFullMessageTableName() + " M, "
                                              + getFullAckTableName() + " D "
                                              + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                              + " AND M.CONTAINER=D.CONTAINER AND M.ID > ?"
                                              + " ORDER BY M.ID";
        }
        return findDurableSubMessagesStatement;
    }

    public String findAllDurableSubMessagesStatement() {
        if (findAllDurableSubMessagesStatement == null) {
            findAllDurableSubMessagesStatement = "SELECT M.ID, M.MSG FROM " + getFullMessageTableName()
                                                 + " M, " + getFullAckTableName() + " D "
                                                 + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                                 + " AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID"
                                                 + " ORDER BY M.ID";
        }
        return findAllDurableSubMessagesStatement;
    }

    public String getNextDurableSubscriberMessageStatement() {
        if (nextDurableSubscriberMessageStatement == null) {
            nextDurableSubscriberMessageStatement = "SELECT M.ID, M.MSG FROM "
                                                    + getFullMessageTableName()
                                                    + " M, "
                                                    + getFullAckTableName()
                                                    + " D "
                                                    + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                                    + " AND M.CONTAINER=D.CONTAINER AND M.ID > ?"
                                                    + " ORDER BY M.ID ";
        }
        return nextDurableSubscriberMessageStatement;
    }

    /**
     * @return the durableSubscriberMessageCountStatement
     */

    public String getDurableSubscriberMessageCountStatement() {
        if (durableSubscriberMessageCountStatement == null) {
            durableSubscriberMessageCountStatement = "SELECT COUNT(*) FROM "
                                                     + getFullMessageTableName()
                                                     + " M, "
                                                     + getFullAckTableName()
                                                     + " D "
                                                     + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                                     + " AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID";
        }
        return durableSubscriberMessageCountStatement;
    }

    public String getFindAllDestinationsStatement() {
        if (findAllDestinationsStatement == null) {
            findAllDestinationsStatement = "SELECT DISTINCT CONTAINER FROM " + getFullMessageTableName();
        }
        return findAllDestinationsStatement;
    }

    public String getRemoveAllMessagesStatement() {
        if (removeAllMessagesStatement == null) {
            removeAllMessagesStatement = "DELETE FROM " + getFullMessageTableName() + " WHERE CONTAINER=?";
        }
        return removeAllMessagesStatement;
    }

    public String getRemoveAllSubscriptionsStatement() {
        if (removeAllSubscriptionsStatement == null) {
            removeAllSubscriptionsStatement = "DELETE FROM " + getFullAckTableName() + " WHERE CONTAINER=?";
        }
        return removeAllSubscriptionsStatement;
    }

    public String getDeleteOldMessagesStatement() {
        if (deleteOldMessagesStatement == null) {
            deleteOldMessagesStatement = "DELETE FROM " + getFullMessageTableName()
                                         + " WHERE ( EXPIRATION<>0 AND EXPIRATION<?) OR ID <= "
                                         + "( SELECT min(" + getFullAckTableName() + ".LAST_ACKED_ID) "
                                         + "FROM " + getFullAckTableName() + " WHERE "
                                         + getFullAckTableName() + ".CONTAINER=" + getFullMessageTableName()
                                         + ".CONTAINER)";
        }
        return deleteOldMessagesStatement;
    }

    public String getLockCreateStatement() {
        if (lockCreateStatement == null) {
            lockCreateStatement = "SELECT * FROM " + getFullLockTableName();
            if (useLockCreateWhereClause) {
                lockCreateStatement += " WHERE ID = 1";
            }
            lockCreateStatement += " FOR UPDATE";
        }
        return lockCreateStatement;
    }

    public String getLockUpdateStatement() {
        if (lockUpdateStatement == null) {
            lockUpdateStatement = "UPDATE " + getFullLockTableName() + " SET TIME = ? WHERE ID = 1";
        }
        return lockUpdateStatement;
    }

    /**
     * @return the destinationMessageCountStatement
     */
    public String getDestinationMessageCountStatement() {
        if (destinationMessageCountStatement == null) {
            destinationMessageCountStatement = "SELECT COUNT(*) FROM " + getFullMessageTableName()
                                               + " WHERE CONTAINER=?";
        }
        return destinationMessageCountStatement;
    }

    /**
     * @return the findNextMessagesStatement
     */
    public String getFindNextMessagesStatement() {
        if (findNextMessagesStatement == null) {
            findNextMessagesStatement = "SELECT ID, MSG FROM " + getFullMessageTableName()
                                        + " WHERE CONTAINER=? AND ID > ? ORDER BY ID";
        }
        return findNextMessagesStatement;
    }

    /**
     * @return the lastAckedDurableSubscriberMessageStatement
     */
    public String getLastAckedDurableSubscriberMessageStatement() {
        if (lastAckedDurableSubscriberMessageStatement == null) {
            lastAckedDurableSubscriberMessageStatement = "SELECT MAX(LAST_ACKED_ID) FROM "
                                                         + getFullAckTableName()
                                                         + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return lastAckedDurableSubscriberMessageStatement;
    }

    public String getFullMessageTableName() {
        return getTablePrefix() + getMessageTableName();
    }

    public String getFullAckTableName() {
        return getTablePrefix() + getDurableSubAcksTableName();
    }

    public String getFullLockTableName() {
        return getTablePrefix() + getLockTableName();
    }

    /**
     * @return Returns the containerNameDataType.
     */
    public String getContainerNameDataType() {
        return containerNameDataType;
    }

    /**
     * @param containerNameDataType The containerNameDataType to set.
     */
    public void setContainerNameDataType(String containerNameDataType) {
        this.containerNameDataType = containerNameDataType;
    }

    /**
     * @return Returns the messageDataType.
     */
    public String getBinaryDataType() {
        return binaryDataType;
    }

    /**
     * @param messageDataType The messageDataType to set.
     */
    public void setBinaryDataType(String messageDataType) {
        this.binaryDataType = messageDataType;
    }

    /**
     * @return Returns the messageTableName.
     */
    public String getMessageTableName() {
        return messageTableName;
    }

    /**
     * @param messageTableName The messageTableName to set.
     */
    public void setMessageTableName(String messageTableName) {
        this.messageTableName = messageTableName;
    }

    /**
     * @return Returns the msgIdDataType.
     */
    public String getMsgIdDataType() {
        return msgIdDataType;
    }

    /**
     * @param msgIdDataType The msgIdDataType to set.
     */
    public void setMsgIdDataType(String msgIdDataType) {
        this.msgIdDataType = msgIdDataType;
    }

    /**
     * @return Returns the sequenceDataType.
     */
    public String getSequenceDataType() {
        return sequenceDataType;
    }

    /**
     * @param sequenceDataType The sequenceDataType to set.
     */
    public void setSequenceDataType(String sequenceDataType) {
        this.sequenceDataType = sequenceDataType;
    }

    /**
     * @return Returns the tablePrefix.
     */
    public String getTablePrefix() {
        return tablePrefix;
    }

    /**
     * @param tablePrefix The tablePrefix to set.
     */
    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    /**
     * @return Returns the durableSubAcksTableName.
     */
    public String getDurableSubAcksTableName() {
        return durableSubAcksTableName;
    }

    /**
     * @param durableSubAcksTableName The durableSubAcksTableName to set.
     */
    public void setDurableSubAcksTableName(String durableSubAcksTableName) {
        this.durableSubAcksTableName = durableSubAcksTableName;
    }

    public String getLockTableName() {
        return lockTableName;
    }

    public void setLockTableName(String lockTableName) {
        this.lockTableName = lockTableName;
    }

    public String getLongDataType() {
        return longDataType;
    }

    public void setLongDataType(String longDataType) {
        this.longDataType = longDataType;
    }

    public String getStringIdDataType() {
        return stringIdDataType;
    }

    public void setStringIdDataType(String stringIdDataType) {
        this.stringIdDataType = stringIdDataType;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.useExternalMessageReferences = useExternalMessageReferences;
    }

    public boolean isUseExternalMessageReferences() {
        return useExternalMessageReferences;
    }

    public void setAddMessageStatement(String addMessageStatment) {
        this.addMessageStatement = addMessageStatment;
    }

    public void setCreateDurableSubStatement(String createDurableSubStatment) {
        this.createDurableSubStatement = createDurableSubStatment;
    }

    public void setCreateSchemaStatements(String[] createSchemaStatments) {
        this.createSchemaStatements = createSchemaStatments;
    }

    public void setDeleteOldMessagesStatement(String deleteOldMessagesStatment) {
        this.deleteOldMessagesStatement = deleteOldMessagesStatment;
    }

    public void setDeleteSubscriptionStatement(String deleteSubscriptionStatment) {
        this.deleteSubscriptionStatement = deleteSubscriptionStatment;
    }

    public void setDropSchemaStatements(String[] dropSchemaStatments) {
        this.dropSchemaStatements = dropSchemaStatments;
    }

    public void setFindAllDestinationsStatement(String findAllDestinationsStatment) {
        this.findAllDestinationsStatement = findAllDestinationsStatment;
    }

    public void setFindAllDurableSubMessagesStatement(String findAllDurableSubMessagesStatment) {
        this.findAllDurableSubMessagesStatement = findAllDurableSubMessagesStatment;
    }

    public void setFindAllDurableSubsStatement(String findAllDurableSubsStatment) {
        this.findAllDurableSubsStatement = findAllDurableSubsStatment;
    }

    public void setFindAllMessagesStatement(String findAllMessagesStatment) {
        this.findAllMessagesStatement = findAllMessagesStatment;
    }

    public void setFindDurableSubStatement(String findDurableSubStatment) {
        this.findDurableSubStatement = findDurableSubStatment;
    }

    public void setFindLastSequenceIdInAcksStatement(String findLastSequenceIdInAcks) {
        this.findLastSequenceIdInAcksStatement = findLastSequenceIdInAcks;
    }

    public void setFindLastSequenceIdInMsgsStatement(String findLastSequenceIdInMsgs) {
        this.findLastSequenceIdInMsgsStatement = findLastSequenceIdInMsgs;
    }

    public void setFindMessageSequenceIdStatement(String findMessageSequenceIdStatment) {
        this.findMessageSequenceIdStatement = findMessageSequenceIdStatment;
    }

    public void setFindMessageStatement(String findMessageStatment) {
        this.findMessageStatement = findMessageStatment;
    }

    public void setRemoveAllMessagesStatement(String removeAllMessagesStatment) {
        this.removeAllMessagesStatement = removeAllMessagesStatment;
    }

    public void setRemoveAllSubscriptionsStatement(String removeAllSubscriptionsStatment) {
        this.removeAllSubscriptionsStatement = removeAllSubscriptionsStatment;
    }

    public void setRemoveMessageStatment(String removeMessageStatment) {
        this.removeMessageStatment = removeMessageStatment;
    }

    public void setUpdateLastAckOfDurableSubStatement(String updateLastAckOfDurableSub) {
        this.updateLastAckOfDurableSubStatement = updateLastAckOfDurableSub;
    }

    public void setUpdateMessageStatement(String updateMessageStatment) {
        this.updateMessageStatement = updateMessageStatment;
    }

    public boolean isUseLockCreateWhereClause() {
        return useLockCreateWhereClause;
    }

    public void setUseLockCreateWhereClause(boolean useLockCreateWhereClause) {
        this.useLockCreateWhereClause = useLockCreateWhereClause;
    }

    public void setLockCreateStatement(String lockCreateStatement) {
        this.lockCreateStatement = lockCreateStatement;
    }

    public void setLockUpdateStatement(String lockUpdateStatement) {
        this.lockUpdateStatement = lockUpdateStatement;
    }

    /**
     * @param findDurableSubMessagesStatement the
     *                findDurableSubMessagesStatement to set
     */
    public void setFindDurableSubMessagesStatement(String findDurableSubMessagesStatement) {
        this.findDurableSubMessagesStatement = findDurableSubMessagesStatement;
    }

    /**
     * @param nextDurableSubscriberMessageStatement the nextDurableSubscriberMessageStatement to set
     */
    public void setNextDurableSubscriberMessageStatement(String nextDurableSubscriberMessageStatement) {
        this.nextDurableSubscriberMessageStatement = nextDurableSubscriberMessageStatement;
    }

    /**
     * @param durableSubscriberMessageCountStatement the durableSubscriberMessageCountStatement to set
     */
    public void setDurableSubscriberMessageCountStatement(String durableSubscriberMessageCountStatement) {
        this.durableSubscriberMessageCountStatement = durableSubscriberMessageCountStatement;
    }

    /**
     * @param findNextMessagesStatement the findNextMessagesStatement to set
     */
    public void setFindNextMessagesStatement(String findNextMessagesStatement) {
        this.findNextMessagesStatement = findNextMessagesStatement;
    }

    /**
     * @param destinationMessageCountStatement the destinationMessageCountStatement to set
     */
    public void setDestinationMessageCountStatement(String destinationMessageCountStatement) {
        this.destinationMessageCountStatement = destinationMessageCountStatement;
    }

    /**
     * @param lastAckedDurableSubscriberMessageStatement the lastAckedDurableSubscriberMessageStatement to set
     */
    public void setLastAckedDurableSubscriberMessageStatement(
                                                              String lastAckedDurableSubscriberMessageStatement) {
        this.lastAckedDurableSubscriberMessageStatement = lastAckedDurableSubscriberMessageStatement;
    }

}