/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.quick;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

final class RecoveryListenerAdapter implements MessageRecoveryListener {
	static final private Log log = LogFactory.getLog(RecoveryListenerAdapter.class);
	
	private final MessageStore store;
	private final MessageRecoveryListener listener;

	RecoveryListenerAdapter(MessageStore store, MessageRecoveryListener listener) {
		this.store = store;
		this.listener = listener;
	}

	public void finished() {
		listener.finished();
	}

	public boolean hasSpace() {
		return listener.hasSpace();
	}

	public void recoverMessage(Message message) throws Exception {
		listener.recoverMessage(message);
	}

	public void recoverMessageReference(MessageId ref) throws Exception {
		Message message = this.store.getMessage(ref);
		if( message !=null ){
			listener.recoverMessage( message );
		} else {
			log.error("Message id "+ref+" could not be recovered from the data store!");
		}
			
	}
}