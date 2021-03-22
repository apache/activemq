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
 * 
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
    protected String sequenceDataType = "BIGINT";
    protected String longDataType = "BIGINT";
    protected String stringIdDataType = "VARCHAR(250)";
    protected boolean useExternalMessageReferences;

    private String tablePrefix = "";
    private String addMessageStatement;
    private String updateMessageStatement;
    private String removeMessageStatement;
    private String findMessageSequenceIdStatement;
    private String findMessageStatement;
    private String findMessageByIdStatement;
    private String findAllMessagesStatement;
    private String findLastSequenceIdInMsgsStatement;
    private String findLastSequenceIdInAcksStatement;
    private String createDurableSubStatement;
    private String findDurableSubStatement;
    private String findAllDurableSubsStatement;
    private String updateLastPriorityAckRowOfDurableSubStatement;
    private String deleteSubscriptionStatement;
    private String findAllDurableSubMessagesStatement;
    private String findDurableSubMessagesStatement;
    private String findDurableSubMessagesByPriorityStatement;
    private String findAllDestinationsStatement;
    private String removeAllMessagesStatement;
    private String removeAllSubscriptionsStatement;
    private String[] createSchemaStatements;
    private String[] createLockSchemaStatements;
    private String[] dropSchemaStatements;
    private String lockCreateStatement;
    private String lockUpdateStatement;
    private String nextDurableSubscriberMessageStatement;
    private String durableSubscriberMessageCountStatement;
    private String lastAckedDurableSubscriberMessageStatement;
    private String destinationMessageCountStatement;
    private String findNextMessagesStatement;
    private String findNextMessagesByPriorityStatement;
    private boolean useLockCreateWhereClause;
    private String findAllMessageIdsStatement;
    private String lastProducerSequenceIdStatement;
    private String selectDurablePriorityAckStatement;

    private String insertDurablePriorityAckStatement;
    private String updateDurableLastAckStatement;
    private String deleteOldMessagesStatementWithPriority;
    private String durableSubscriberMessageCountStatementWithPriority;
    private String dropAckPKAlterStatementEnd;
    private String updateXidFlagStatement;
    private String findOpsPendingOutcomeStatement;
    private String clearXidFlagStatement;
    private String updateDurableLastAckInTxStatement;
    private String findAcksPendingOutcomeStatement;
    private String clearDurableLastAckInTxStatement;
    private String updateDurableLastAckWithPriorityStatement;
    private String updateDurableLastAckWithPriorityInTxStatement;
    private String findXidByIdStatement;
    private String leaseObtainStatement;
    private String currentDateTimeStatement;
    private String leaseUpdateStatement;
    private String leaseOwnerStatement;

    public String[] getCreateSchemaStatements() {
        if (createSchemaStatements == null) {
            createSchemaStatements = new String[] {
                "CREATE TABLE " + getFullMessageTableName() + "(" + "ID " + sequenceDataType + " NOT NULL"
                    + ", CONTAINER " + containerNameDataType + " NOT NULL, MSGID_PROD " + msgIdDataType + ", MSGID_SEQ "
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
                    + ", CONSTRAINT PK_" + getDurableSubAcksTableName() + " PRIMARY KEY ( CONTAINER, CLIENT_ID, SUB_NAME))",
                "ALTER TABLE " + getFullMessageTableName() + " ADD PRIORITY " + sequenceDataType,
                "CREATE INDEX " + getFullMessageTableName() + "_PIDX ON " + getFullMessageTableName() + " (PRIORITY)",
                "ALTER TABLE " + getFullMessageTableName() + " ADD XID " + stringIdDataType,
                "ALTER TABLE " + getFullAckTableName() + " ADD PRIORITY " + sequenceDataType  + " DEFAULT 5 NOT NULL",
                "ALTER TABLE " + getFullAckTableName() + " ADD XID " + stringIdDataType,
                "ALTER TABLE " + getFullAckTableName() + " " + getDropAckPKAlterStatementEnd(),
                "ALTER TABLE " + getFullAckTableName() + " ADD PRIMARY KEY (CONTAINER, CLIENT_ID, SUB_NAME, PRIORITY)",
                "CREATE INDEX " + getFullMessageTableName() + "_XIDX ON " + getFullMessageTableName() + " (XID)",
                "CREATE INDEX " + getFullAckTableName() + "_XIDX ON " + getFullAckTableName() + " (XID)",
                "CREATE INDEX " + getFullMessageTableName() + "_IIDX ON " + getFullMessageTableName() + " (ID ASC, XID, CONTAINER)"
            };
        }
        getCreateLockSchemaStatements();
        String[] allCreateStatements = new String[createSchemaStatements.length + createLockSchemaStatements.length];
        System.arraycopy(createSchemaStatements, 0, allCreateStatements, 0, createSchemaStatements.length);
        System.arraycopy(createLockSchemaStatements, 0, allCreateStatements, createSchemaStatements.length, createLockSchemaStatements.length);

        return allCreateStatements;
    }

    public String[] getCreateLockSchemaStatements() {
        if (createLockSchemaStatements == null) {
            createLockSchemaStatements = new String[] {
                "CREATE TABLE " + getFullLockTableName()
                    + "( ID " + longDataType + " NOT NULL, TIME " + longDataType
                    + ", BROKER_NAME " + stringIdDataType + ", PRIMARY KEY (ID) )",
                "INSERT INTO " + getFullLockTableName() + "(ID) VALUES (1)"
            };
        }
        return createLockSchemaStatements;
    }

    public String getDropAckPKAlterStatementEnd() {
        if (dropAckPKAlterStatementEnd == null) {
            dropAckPKAlterStatementEnd = "DROP PRIMARY KEY";
        }
        return dropAckPKAlterStatementEnd;
    }

    public void setDropAckPKAlterStatementEnd(String dropAckPKAlterStatementEnd) {
        this.dropAckPKAlterStatementEnd = dropAckPKAlterStatementEnd;
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
                                  + "(ID, MSGID_PROD, MSGID_SEQ, CONTAINER, EXPIRATION, PRIORITY, MSG, XID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        }
        return addMessageStatement;
    }

    public String getUpdateMessageStatement() {
        if (updateMessageStatement == null) {
            updateMessageStatement = "UPDATE " + getFullMessageTableName() + " SET MSG=? WHERE MSGID_PROD=? AND MSGID_SEQ=? AND CONTAINER=?";
        }
        return updateMessageStatement;
    }

    public String getRemoveMessageStatement() {
        if (removeMessageStatement == null) {
            removeMessageStatement = "DELETE FROM " + getFullMessageTableName() + " WHERE ID=?";
        }
        return removeMessageStatement;
    }

    public String getFindMessageSequenceIdStatement() {
        if (findMessageSequenceIdStatement == null) {
            findMessageSequenceIdStatement = "SELECT ID, PRIORITY FROM " + getFullMessageTableName()
                                             + " WHERE MSGID_PROD=? AND MSGID_SEQ=? AND CONTAINER=?";
        }
        return findMessageSequenceIdStatement;
    }

    public String getFindMessageStatement() {
        if (findMessageStatement == null) {
            findMessageStatement = "SELECT MSG FROM " + getFullMessageTableName() + " WHERE MSGID_PROD=? AND MSGID_SEQ=?";
        }
        return findMessageStatement;
    }

    public String getFindMessageByIdStatement() {
        if (findMessageByIdStatement == null) {
        	findMessageByIdStatement = "SELECT MSG FROM " + getFullMessageTableName() + " WHERE ID=?";
        }
        return findMessageByIdStatement;
    }

    public String getFindXidByIdStatement() {
        if (findXidByIdStatement == null) {
            findXidByIdStatement = "SELECT XID FROM " + getFullMessageTableName() + " WHERE ID=?";
        }
        return findXidByIdStatement;
    }

    public String getFindAllMessagesStatement() {
        if (findAllMessagesStatement == null) {
            findAllMessagesStatement = "SELECT ID, MSG FROM " + getFullMessageTableName()
                                       + " WHERE CONTAINER=? ORDER BY ID";
        }
        return findAllMessagesStatement;
    }
    
    public String getFindAllMessageIdsStatement() {
        //  this needs to be limited maybe need to use getFindLastSequenceIdInMsgsStatement
        // and work back for X
        if (findAllMessageIdsStatement == null) {
            findAllMessageIdsStatement = "SELECT ID, MSGID_PROD, MSGID_SEQ FROM " + getFullMessageTableName()
                                       + " ORDER BY ID DESC";
        }
        return findAllMessageIdsStatement;
    }

    public void setFindAllMessageIdsStatement(String val) {
        findAllMessageIdsStatement = val;
    }

    public String getFindLastSequenceIdInMsgsStatement() {
        if (findLastSequenceIdInMsgsStatement == null) {
            findLastSequenceIdInMsgsStatement = "SELECT MAX(ID) FROM " + getFullMessageTableName();
        }
        return findLastSequenceIdInMsgsStatement;
    }

    public String getLastProducerSequenceIdStatement() {
        if (lastProducerSequenceIdStatement == null) {
            lastProducerSequenceIdStatement = "SELECT MAX(MSGID_SEQ) FROM " + getFullMessageTableName()
                                            + " WHERE MSGID_PROD=?";
        }
        return lastProducerSequenceIdStatement;
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
                                        + "(CONTAINER, CLIENT_ID, SUB_NAME, SELECTOR, LAST_ACKED_ID, SUB_DEST, PRIORITY) "
                                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
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
                                          + getFullAckTableName() + " WHERE CONTAINER=? AND PRIORITY=0";
        }
        return findAllDurableSubsStatement;
    }

    public String getUpdateLastPriorityAckRowOfDurableSubStatement() {
        if (updateLastPriorityAckRowOfDurableSubStatement == null) {
            updateLastPriorityAckRowOfDurableSubStatement = "UPDATE " + getFullAckTableName() + " SET LAST_ACKED_ID=?"
                                                 + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=? AND PRIORITY=?";
        }
        return updateLastPriorityAckRowOfDurableSubStatement;
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
                                                 + " ORDER BY M.PRIORITY DESC, M.ID";
        }
        return findAllDurableSubMessagesStatement;
    }

    public String getFindDurableSubMessagesStatement() {
        if (findDurableSubMessagesStatement == null) {
            findDurableSubMessagesStatement = "SELECT M.ID, M.MSG FROM " + getFullMessageTableName() + " M, "
                                              + getFullAckTableName() + " D "
                                              + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                              + " AND M.XID IS NULL"
                                              + " AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID"
                                              + " AND M.ID > ?"
                                              + " ORDER BY M.ID";
        }
        return findDurableSubMessagesStatement;
    }
    
    public String getFindDurableSubMessagesByPriorityStatement() {
        if (findDurableSubMessagesByPriorityStatement == null) {
            findDurableSubMessagesByPriorityStatement = "SELECT M.ID, M.MSG FROM " + getFullMessageTableName() + " M,"
                                              + " " + getFullAckTableName() + " D"
                                              + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                              + " AND M.XID IS NULL"
                                              + " AND M.CONTAINER=D.CONTAINER"
                                              + " AND M.PRIORITY=D.PRIORITY AND M.ID > D.LAST_ACKED_ID"
                                              + " AND M.ID > ? AND M.PRIORITY = ?"
                                              + " ORDER BY M.ID";
        }
        return findDurableSubMessagesByPriorityStatement;
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
                                                     + " AND M.CONTAINER=D.CONTAINER "
                                                     + "     AND M.ID >"
                                                     + "          ( SELECT LAST_ACKED_ID FROM " + getFullAckTableName()
                                                     + "           WHERE CONTAINER=D.CONTAINER AND CLIENT_ID=D.CLIENT_ID"
                                                     + "           AND SUB_NAME=D.SUB_NAME )";

        }
        return durableSubscriberMessageCountStatement;
    }

    public String getDurableSubscriberMessageCountStatementWithPriority() {
        if (durableSubscriberMessageCountStatementWithPriority == null) {
            durableSubscriberMessageCountStatementWithPriority = "SELECT COUNT(*) FROM "
                                                     + getFullMessageTableName()
                                                     + " M, "
                                                     + getFullAckTableName()
                                                     + " D "
                                                     + " WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?"
                                                     + " AND M.CONTAINER=D.CONTAINER "
                                                     + " AND M.PRIORITY=D.PRIORITY "
                                                     + " AND M.ID > D.LAST_ACKED_ID";
        }

        return durableSubscriberMessageCountStatementWithPriority;
    }

    public String getFindAllDestinationsStatement() {
        if (findAllDestinationsStatement == null) {
            findAllDestinationsStatement = "SELECT DISTINCT CONTAINER FROM " + getFullMessageTableName()
                    + " WHERE CONTAINER IS NOT NULL UNION SELECT DISTINCT CONTAINER FROM " + getFullAckTableName();
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

    public String getDeleteOldMessagesStatementWithPriority() {
        if (deleteOldMessagesStatementWithPriority == null) {
            deleteOldMessagesStatementWithPriority = "DELETE FROM " + getFullMessageTableName()
                                         + " WHERE (PRIORITY=? AND ID <= "
                                         + "     ( SELECT min(" + getFullAckTableName() + ".LAST_ACKED_ID)"
                                         + "       FROM " + getFullAckTableName() + " WHERE "
                                         +          getFullAckTableName() + ".CONTAINER="
                                         +          getFullMessageTableName() + ".CONTAINER"
                                         + "        AND " + getFullAckTableName() + ".PRIORITY=?)"
                                         + "   )";
        }
        return deleteOldMessagesStatementWithPriority;
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

    public String getLeaseObtainStatement() {
        if (leaseObtainStatement == null) {
            leaseObtainStatement = "UPDATE " + getFullLockTableName()
                    + " SET BROKER_NAME=?, TIME=?"
                    + " WHERE (TIME IS NULL OR TIME < ?) AND ID = 1";
        }
        return leaseObtainStatement;
    }

    public String getCurrentDateTime() {
        if (currentDateTimeStatement == null) {
            currentDateTimeStatement = "SELECT CURRENT_TIMESTAMP FROM " + getFullLockTableName();
        }
        return currentDateTimeStatement;
    }

    public String getLeaseUpdateStatement() {
        if (leaseUpdateStatement == null) {
            leaseUpdateStatement = "UPDATE " + getFullLockTableName()
                    + " SET BROKER_NAME=?, TIME=?"
                    + " WHERE BROKER_NAME=? AND ID = 1";
        }
        return leaseUpdateStatement;
    }

    public String getLeaseOwnerStatement() {
        if (leaseOwnerStatement == null) {
            leaseOwnerStatement = "SELECT BROKER_NAME, TIME FROM " + getFullLockTableName()
                    + " WHERE ID = 1";
        }
        return leaseOwnerStatement;
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
                                               + " WHERE CONTAINER=? AND XID IS NULL";
        }
        return destinationMessageCountStatement;
    }

    /**
     * @return the findNextMessagesStatement
     */
    public String getFindNextMessagesStatement() {
        if (findNextMessagesStatement == null) {
            findNextMessagesStatement = "SELECT ID, MSG FROM " + getFullMessageTableName()
                                        + " WHERE CONTAINER=? AND ID < ? AND ID > ? AND XID IS NULL ORDER BY ID";
        }
        return findNextMessagesStatement;
    }

    /**
     * @return the findNextMessagesStatement
     */
    public String getFindNextMessagesByPriorityStatement() {
        if (findNextMessagesByPriorityStatement == null) {
            findNextMessagesByPriorityStatement = "SELECT ID, MSG FROM " + getFullMessageTableName()
                                        + " WHERE CONTAINER=?"
                                        + " AND XID IS NULL"
                                        + " AND ID < ? "
                                        + " AND ( (ID > ? AND PRIORITY = 9) "
                                        + "    OR (ID > ? AND PRIORITY = 8) "
                                        + "    OR (ID > ? AND PRIORITY = 7) "
                                        + "    OR (ID > ? AND PRIORITY = 6) "
                                        + "    OR (ID > ? AND PRIORITY = 5) "
                                        + "    OR (ID > ? AND PRIORITY = 4) "
                                        + "    OR (ID > ? AND PRIORITY = 3) "
                                        + "    OR (ID > ? AND PRIORITY = 2) "
                                        + "    OR (ID > ? AND PRIORITY = 1) "
                                        + "    OR (ID > ? AND PRIORITY = 0) )"
                                        + " ORDER BY PRIORITY DESC, ID";
        }
        return findNextMessagesByPriorityStatement;
    }    

    public void setFindNextMessagesByPriorityStatement(String val) {
        findNextMessagesByPriorityStatement = val;
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

    public String getSelectDurablePriorityAckStatement() {
        if (selectDurablePriorityAckStatement == null) {
            selectDurablePriorityAckStatement = "SELECT LAST_ACKED_ID FROM " + getFullAckTableName()
                                                    + " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?"
                                                    + " AND PRIORITY = ?";
        }
        return selectDurablePriorityAckStatement;
    }

    public String getInsertDurablePriorityAckStatement() {
        if (insertDurablePriorityAckStatement == null) {
            insertDurablePriorityAckStatement = "INSERT INTO "
                                  + getFullAckTableName()
                                  + "(CONTAINER, CLIENT_ID, SUB_NAME, PRIORITY)"
                                  + " VALUES (?, ?, ?, ?)";            
       }
        return insertDurablePriorityAckStatement;
    }


    public String getUpdateDurableLastAckStatement() {
        if (updateDurableLastAckStatement == null) {
            updateDurableLastAckStatement  = "UPDATE " + getFullAckTableName()
                    + " SET LAST_ACKED_ID=?, XID = NULL WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return  updateDurableLastAckStatement;
    }

    public String getUpdateDurableLastAckInTxStatement() {
        if (updateDurableLastAckInTxStatement == null) {
            updateDurableLastAckInTxStatement = "UPDATE " + getFullAckTableName()
                    + " SET XID=? WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
        }
        return updateDurableLastAckInTxStatement;
    }

    public String getUpdateDurableLastAckWithPriorityStatement() {
        if (updateDurableLastAckWithPriorityStatement == null) {
            updateDurableLastAckWithPriorityStatement  = "UPDATE " + getFullAckTableName()
                    + " SET LAST_ACKED_ID=?, XID = NULL WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=? AND PRIORITY=?";
        }
        return  updateDurableLastAckWithPriorityStatement;
    }

    public String getUpdateDurableLastAckWithPriorityInTxStatement() {
        if (updateDurableLastAckWithPriorityInTxStatement == null) {
            updateDurableLastAckWithPriorityInTxStatement  = "UPDATE " + getFullAckTableName()
                    + " SET XID=? WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=? AND PRIORITY=?";
        }
        return  updateDurableLastAckWithPriorityInTxStatement;
    }

    public String getClearDurableLastAckInTxStatement() {
        if (clearDurableLastAckInTxStatement == null) {
            clearDurableLastAckInTxStatement = "UPDATE " + getFullAckTableName()
                    + " SET XID = NULL WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=? AND PRIORITY=?";
        }
        return clearDurableLastAckInTxStatement;
    }

    public String getFindOpsPendingOutcomeStatement() {
        if (findOpsPendingOutcomeStatement == null) {
            findOpsPendingOutcomeStatement = "SELECT ID, XID, MSG FROM " + getFullMessageTableName()
                    + " WHERE XID IS NOT NULL ORDER BY ID";
        }
        return findOpsPendingOutcomeStatement;
    }

    public String getFindAcksPendingOutcomeStatement() {
        if (findAcksPendingOutcomeStatement == null) {
            findAcksPendingOutcomeStatement = "SELECT XID," +
                    " CONTAINER, CLIENT_ID, SUB_NAME FROM " + getFullAckTableName()
                    + " WHERE XID IS NOT NULL";
        }
        return findAcksPendingOutcomeStatement;
    }

    public String getUpdateXidFlagStatement() {
        if (updateXidFlagStatement == null) {
            updateXidFlagStatement = "UPDATE " + getFullMessageTableName()
                    + " SET XID = ? WHERE ID = ?";
        }
        return updateXidFlagStatement;
    }

    public String getClearXidFlagStatement() {
        if (clearXidFlagStatement == null) {
            clearXidFlagStatement = "UPDATE "  + getFullMessageTableName()
                    + " SET XID = NULL, ID = ? WHERE ID = ?";
        }
        return clearXidFlagStatement;
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

    public void setCreateLockSchemaStatements(String[] createLockSchemaStatments) {
        this.createLockSchemaStatements = createLockSchemaStatments;
    }

    public void setDeleteOldMessagesStatementWithPriority(String deleteOldMessagesStatementWithPriority) {
        this.deleteOldMessagesStatementWithPriority = deleteOldMessagesStatementWithPriority;
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
    
    public void setFindMessageByIdStatement(String findMessageByIdStatement) {
        this.findMessageByIdStatement = findMessageByIdStatement;
    }

    public void setRemoveAllMessagesStatement(String removeAllMessagesStatment) {
        this.removeAllMessagesStatement = removeAllMessagesStatment;
    }

    public void setRemoveAllSubscriptionsStatement(String removeAllSubscriptionsStatment) {
        this.removeAllSubscriptionsStatement = removeAllSubscriptionsStatment;
    }

    public void setRemoveMessageStatment(String removeMessageStatement) {
        this.removeMessageStatement = removeMessageStatement;
    }

    public void setUpdateLastPriorityAckRowOfDurableSubStatement(String updateLastPriorityAckRowOfDurableSubStatement) {
        this.updateLastPriorityAckRowOfDurableSubStatement = updateLastPriorityAckRowOfDurableSubStatement;
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

    public void setDurableSubscriberMessageCountStatementWithPriority(String durableSubscriberMessageCountStatementWithPriority) {
        this.durableSubscriberMessageCountStatementWithPriority = durableSubscriberMessageCountStatementWithPriority;
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


    public void setLastProducerSequenceIdStatement(String lastProducerSequenceIdStatement) {
        this.lastProducerSequenceIdStatement = lastProducerSequenceIdStatement;
    }

    public void setSelectDurablePriorityAckStatement(String selectDurablePriorityAckStatement) {
        this.selectDurablePriorityAckStatement = selectDurablePriorityAckStatement;
    }

    public void setInsertDurablePriorityAckStatement(String insertDurablePriorityAckStatement) {
        this.insertDurablePriorityAckStatement = insertDurablePriorityAckStatement;
    }

    public void setUpdateDurableLastAckStatement(String updateDurableLastAckStatement) {
        this.updateDurableLastAckStatement = updateDurableLastAckStatement;
    }

    public void setUpdateXidFlagStatement(String updateXidFlagStatement) {
        this.updateXidFlagStatement = updateXidFlagStatement;
    }

    public void setFindOpsPendingOutcomeStatement(String findOpsPendingOutcomeStatement) {
        this.findOpsPendingOutcomeStatement = findOpsPendingOutcomeStatement;
    }

    public void setClearXidFlagStatement(String clearXidFlagStatement) {
        this.clearXidFlagStatement = clearXidFlagStatement;
    }

    public void setUpdateDurableLastAckInTxStatement(String updateDurableLastAckInTxStatement) {
        this.updateDurableLastAckInTxStatement = updateDurableLastAckInTxStatement;
    }

    public void setFindAcksPendingOutcomeStatement(String findAcksPendingOutcomeStatement) {
        this.findAcksPendingOutcomeStatement = findAcksPendingOutcomeStatement;
    }

    public void setClearDurableLastAckInTxStatement(String clearDurableLastAckInTxStatement) {
        this.clearDurableLastAckInTxStatement = clearDurableLastAckInTxStatement;
    }

    public void setUpdateDurableLastAckWithPriorityStatement(String updateDurableLastAckWithPriorityStatement) {
        this.updateDurableLastAckWithPriorityStatement = updateDurableLastAckWithPriorityStatement;
    }

    public void setUpdateDurableLastAckWithPriorityInTxStatement(String updateDurableLastAckWithPriorityInTxStatement) {
        this.updateDurableLastAckWithPriorityInTxStatement = updateDurableLastAckWithPriorityInTxStatement;
    }

    public void setFindXidByIdStatement(String findXidByIdStatement) {
        this.findXidByIdStatement = findXidByIdStatement;
    }

    public void setLeaseObtainStatement(String leaseObtainStatement) {
        this.leaseObtainStatement = leaseObtainStatement;
    }

    public void setCurrentDateTimeStatement(String currentDateTimeStatement) {
        this.currentDateTimeStatement = currentDateTimeStatement;
    }

    public void setLeaseUpdateStatement(String leaseUpdateStatement) {
        this.leaseUpdateStatement = leaseUpdateStatement;
    }

    public void setLeaseOwnerStatement(String leaseOwnerStatement) {
        this.leaseOwnerStatement = leaseOwnerStatement;
    }
}
