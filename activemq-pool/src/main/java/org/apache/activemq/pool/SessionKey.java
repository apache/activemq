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
package org.apache.activemq.pool;

/**
 * A cache key for the session details
 *
 * 
 */
public class SessionKey {
    private boolean transacted;
    private int ackMode;
    private int hash;

    public SessionKey(boolean transacted, int ackMode) {
        this.transacted = transacted;
        this.ackMode = ackMode;
        hash = ackMode;
        if (transacted) {
            hash = 31 * hash + 1;
        }
    }

    public int hashCode() {
        return hash;
    }

    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof SessionKey) {
            return equals((SessionKey) that);
        }
        return false;
    }

    public boolean equals(SessionKey that) {
        return this.transacted == that.transacted && this.ackMode == that.ackMode;
    }

    public boolean isTransacted() {
        return transacted;
    }

    public int getAckMode() {
        return ackMode;
    }
}
