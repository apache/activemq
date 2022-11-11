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
package org.apache.activemq.store.journal;

/**
 * Defines an object which listens for Journal Events.
 * 
 * @version $Revision: 1.1 $
 */
public interface JournalEventListener {

	/**
	 * This event is issues when a Journal implementations wants to recover 
	 * disk space used by old records.  If journal space is not reliquised 
	 * by setting the Journal's mark at or past the <code>safeLocation</code>
	 * further write opperations against the Journal may casuse IOExceptions 
	 * to occur due to a log overflow condition.
	 * 
	 * @param safeLocation the oldest location that the journal recomends the mark to be set.
	 */
	void overflowNotification(RecordLocation safeLocation);

}
