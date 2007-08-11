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
package org.apache.activemq.store.kahadaptor;

import org.apache.activemq.kaha.StoreEntry;

/**
 * Holds information for location of message
 * 
 * @version $Revision: 1.10 $
 */
public class TopicSubAck {

    private int count;
    private StoreEntry messageEntry;

    /**
     * @return the count
     */
    public int getCount() {
        return this.count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(int count) {
        this.count = count;
    }

    /**
     * @return the value of the count after it's decremented
     */
    public int decrementCount() {
        return --count;
    }

    /**
     * @return the value of the count after it's incremented
     */
    public int incrementCount() {
        return ++count;
    }

    /**
     * @return the messageEntry
     */
    public StoreEntry getMessageEntry() {
        return this.messageEntry;
    }

    /**
     * @param messageEntry the messageEntry to set
     */
    public void setMessageEntry(StoreEntry storeEntry) {
        this.messageEntry = storeEntry;
    }

}
