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
package org.apache.activemq.store.jdbc.adapter;

import org.apache.activemq.store.jdbc.StatementProvider;

/**
 * A StatementProvider filter that caches the responses
 * of the filtered StatementProvider.
 * 
 * @version $Revision: 1.4 $
 */
public class CachingStatementProvider implements StatementProvider {
    
    final private StatementProvider statementProvider;

    private String addMessageStatment;
    private String[] createSchemaStatments;
    private String[] dropSchemaStatments;
    private String findAllMessagesStatment;
    private String findLastSequenceIdInMsgs;
    private String findMessageStatment;
    private String removeMessageStatment;
    private String updateMessageStatment;
	private String createDurableSubStatment;
	private String findDurableSubStatment;
	private String findAllDurableSubMessagesStatment;
	private String updateLastAckOfDurableSub;
	private String findMessageSequenceIdStatment;
    private String removeAllMessagesStatment;
    private String removeAllSubscriptionsStatment;
    private String deleteSubscriptionStatment;
    private String deleteOldMessagesStatment;
    private String findLastSequenceIdInAcks;
    private String findAllDestinationsStatment;

    public CachingStatementProvider(StatementProvider statementProvider) {
        this.statementProvider = statementProvider;
    }
    
    public StatementProvider getNext() {
        return statementProvider;
    }
    
    public String getAddMessageStatment() {
        if (addMessageStatment == null) {
            addMessageStatment = statementProvider.getAddMessageStatment();
        }
        return addMessageStatment;
    }

    public String[] getCreateSchemaStatments() {
        if( createSchemaStatments==null ) {
            createSchemaStatments = statementProvider.getCreateSchemaStatments();
        }
        return createSchemaStatments;
    }

    public String[] getDropSchemaStatments() {
        if( dropSchemaStatments==null ) {
            dropSchemaStatments = statementProvider.getDropSchemaStatments();
        }
        return dropSchemaStatments;
    }

    public String getFindAllMessagesStatment() {
        if( findAllMessagesStatment==null ) {
            findAllMessagesStatment = statementProvider.getFindAllMessagesStatment();
        }
        return findAllMessagesStatment;
    }

    public String getFindLastSequenceIdInMsgs() {
        if( findLastSequenceIdInMsgs==null ) {
            findLastSequenceIdInMsgs = statementProvider.getFindLastSequenceIdInMsgs();
        }
        return findLastSequenceIdInMsgs;
    }

    public String getFindLastSequenceIdInAcks() {
        if( findLastSequenceIdInAcks==null ) {
            findLastSequenceIdInAcks = statementProvider.getFindLastSequenceIdInAcks();
        }
        return findLastSequenceIdInAcks;
    }

    public String getFindMessageStatment() {
        if( findMessageStatment==null ) {
            findMessageStatment = statementProvider.getFindMessageStatment();
        }
        return findMessageStatment;
    }

    /**
     * @return
     */
    public String getRemoveMessageStatment() {
        if( removeMessageStatment==null ) {
            removeMessageStatment = statementProvider.getRemoveMessageStatment();
        }
        return removeMessageStatment;
    }

    public String getUpdateMessageStatment() {
        if( updateMessageStatment==null ) {
            updateMessageStatment = statementProvider.getUpdateMessageStatment();
        }
        return updateMessageStatment;
    }

	public String getCreateDurableSubStatment() {
		if(createDurableSubStatment==null) {
			createDurableSubStatment = statementProvider.getCreateDurableSubStatment();
		}
		return createDurableSubStatment;
	}

	public String getFindDurableSubStatment() {
		if(findDurableSubStatment==null) {
			findDurableSubStatment = statementProvider.getFindDurableSubStatment();
		}
		return findDurableSubStatment;
	}

	public String getFindAllDurableSubMessagesStatment() {
		if(findAllDurableSubMessagesStatment==null) {
			findAllDurableSubMessagesStatment = statementProvider.getFindAllDurableSubMessagesStatment();
		}
		return findAllDurableSubMessagesStatment;
	}

	public String getUpdateLastAckOfDurableSub() {
		if(updateLastAckOfDurableSub==null) {
			updateLastAckOfDurableSub = statementProvider.getUpdateLastAckOfDurableSub();
		}
		return updateLastAckOfDurableSub;
	}

	public String getFindMessageSequenceIdStatment() {
		if ( findMessageSequenceIdStatment==null ) {
			findMessageSequenceIdStatment = statementProvider.getFindMessageSequenceIdStatment();
		}
		return findMessageSequenceIdStatment;
	}

    public String getRemoveAllMessagesStatment() {
		if ( removeAllMessagesStatment==null ) {
		    removeAllMessagesStatment = statementProvider.getRemoveAllMessagesStatment();
		}
		return removeAllMessagesStatment;
    }

    public String getRemoveAllSubscriptionsStatment() {
		if ( removeAllSubscriptionsStatment==null ) {
		    removeAllSubscriptionsStatment = statementProvider.getRemoveAllSubscriptionsStatment();
		}
		return removeAllSubscriptionsStatment;
    }

    public String getDeleteSubscriptionStatment() {
        if ( deleteSubscriptionStatment==null ) {
            deleteSubscriptionStatment = statementProvider.getDeleteSubscriptionStatment();
        }
        return deleteSubscriptionStatment;
    }

    public String getDeleteOldMessagesStatment() {
        if ( deleteOldMessagesStatment==null ) {
            deleteOldMessagesStatment = statementProvider.getDeleteOldMessagesStatment();
        }
        return deleteOldMessagesStatment;
    }

    public String getFindAllDestinationsStatment() {
        if ( findAllDestinationsStatment==null ) {
            findAllDestinationsStatment = statementProvider.getFindAllDestinationsStatment();
        }
        return findAllDestinationsStatment;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        addMessageStatment=null;
        createSchemaStatments=null;
        dropSchemaStatments=null;
        findAllMessagesStatment=null;
        findLastSequenceIdInMsgs=null;
        findMessageStatment=null;
        removeMessageStatment=null;
        updateMessageStatment=null;
        createDurableSubStatment=null;
        findDurableSubStatment=null;
        findAllDurableSubMessagesStatment=null;
        updateLastAckOfDurableSub=null;
        findMessageSequenceIdStatment=null;
        removeAllMessagesStatment=null;
        removeAllSubscriptionsStatment=null;
        deleteSubscriptionStatment=null;
        deleteOldMessagesStatment=null;
        findLastSequenceIdInAcks=null;
        findAllDestinationsStatment=null;
        statementProvider.setUseExternalMessageReferences(useExternalMessageReferences);
    }

    public boolean isUseExternalMessageReferences() {
        return statementProvider.isUseExternalMessageReferences();
    }
}