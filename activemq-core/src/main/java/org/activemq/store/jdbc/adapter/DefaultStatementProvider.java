/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.store.jdbc.adapter;

import org.activemq.store.jdbc.StatementProvider;


/**
 * @version $Revision: 1.4 $
 */
public class DefaultStatementProvider implements StatementProvider {

    protected String tablePrefix = "";
    protected String messageTableName = "ACTIVEMQ_MSGS";
    protected String durableSubAcksTableName = "ACTIVEMQ_ACKS";

    protected String binaryDataType = "BLOB";
    protected String containerNameDataType = "VARCHAR(250)";
    protected String xidDataType = "VARCHAR(250)";
    protected String msgIdDataType = "VARCHAR(250)";
    protected String sequenceDataType = "INTEGER";
    protected String longDataType = "BIGINT";
    protected String stringIdDataType = "VARCHAR(250)";
    
    protected boolean useExternalMessageReferences=false;

    
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

    public String [] getDropSchemaStatments() {
        return new String[]{
            "DROP TABLE "+tablePrefix+durableSubAcksTableName+"",
            "DROP TABLE "+tablePrefix+messageTableName+"",
        };
    }

    public String getAddMessageStatment() {
        return "INSERT INTO "+tablePrefix+messageTableName+"(ID, MSGID_PROD, MSGID_SEQ, CONTAINER, EXPIRATION, MSG) VALUES (?, ?, ?, ?, ?, ?)";
    }
    public String getUpdateMessageStatment() {
        return "UPDATE "+tablePrefix+messageTableName+" SET MSG=? WHERE ID=?";
    }
    public String getRemoveMessageStatment() {
        return "DELETE FROM "+tablePrefix+messageTableName+" WHERE ID=?";
    }
    public String getFindMessageSequenceIdStatment() {
        return "SELECT ID FROM "+tablePrefix+messageTableName+" WHERE MSGID_PROD=? AND MSGID_SEQ=?";
    }
    public String getFindMessageStatment() {
        return "SELECT MSG FROM "+tablePrefix+messageTableName+" WHERE ID=?";
    }
    public String getFindAllMessagesStatment() {
        return "SELECT ID, MSG FROM "+tablePrefix+messageTableName+" WHERE CONTAINER=? ORDER BY ID";
    }
    public String getFindLastSequenceIdInMsgs() {
        return "SELECT MAX(ID) FROM "+tablePrefix+messageTableName;
    }
    public String getFindLastSequenceIdInAcks() {
        return "SELECT MAX(LAST_ACKED_ID) FROM "+tablePrefix+durableSubAcksTableName;
    }

    public String getCreateDurableSubStatment() {
        return "INSERT INTO "+tablePrefix+durableSubAcksTableName
        	   +"(CONTAINER, CLIENT_ID, SUB_NAME, SELECTOR, LAST_ACKED_ID) "
         	   +"VALUES (?, ?, ?, ?, ?)";
    }

    public String getFindDurableSubStatment() {
        return "SELECT SELECTOR, SUB_NAME " +
                "FROM "+tablePrefix+durableSubAcksTableName+
                " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
    }

    public String getUpdateLastAckOfDurableSub() {
        return "UPDATE "+tablePrefix+durableSubAcksTableName+
                " SET LAST_ACKED_ID=?" +
                " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
    }

    public String getDeleteSubscriptionStatment() {
        return "DELETE FROM "+tablePrefix+durableSubAcksTableName+
                " WHERE CONTAINER=? AND CLIENT_ID=? AND SUB_NAME=?";
    }

    public String getFindAllDurableSubMessagesStatment() {
        return "SELECT M.ID, M.MSG FROM "
		        +tablePrefix+messageTableName+" M, "
			    +tablePrefix+durableSubAcksTableName +" D "
		        +" WHERE D.CONTAINER=? AND D.CLIENT_ID=? AND D.SUB_NAME=?" 
				+" AND M.CONTAINER=D.CONTAINER AND M.ID > D.LAST_ACKED_ID"
				+" ORDER BY M.ID";
    }

    public String getFindAllDestinationsStatment() {
        return "SELECT DISTINCT CONTAINER FROM "+tablePrefix+messageTableName;
    }
    
    public String getRemoveAllMessagesStatment() {
        return "DELETE FROM "+tablePrefix+messageTableName+" WHERE CONTAINER=?";
    }

    public String getRemoveAllSubscriptionsStatment() {
        return "DELETE FROM "+tablePrefix+durableSubAcksTableName+" WHERE CONTAINER=?";
    }

    public String getDeleteOldMessagesStatment() {
        return "DELETE FROM "+tablePrefix+messageTableName+
            " WHERE ( EXPIRATION<>0 AND EXPIRATION<?) OR ID <= " +
            "( SELECT min("+tablePrefix+durableSubAcksTableName+".LAST_ACKED_ID) " +
                "FROM "+tablePrefix+durableSubAcksTableName+" WHERE " +
                tablePrefix+durableSubAcksTableName+".CONTAINER="+tablePrefix+messageTableName+
                ".CONTAINER)";
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
     * @return Returns the xidDataType.
     */
    public String getXidDataType() {
        return xidDataType;
    }
    /**
     * @param xidDataType The xidDataType to set.
     */
    public void setXidDataType(String xidDataType) {
        this.xidDataType = xidDataType;
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
        this.useExternalMessageReferences=useExternalMessageReferences;        
    }

    public boolean isUseExternalMessageReferences() {
        return useExternalMessageReferences;
    }

}