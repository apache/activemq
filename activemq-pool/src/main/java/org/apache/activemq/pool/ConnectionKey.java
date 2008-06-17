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
 * A cache key for the connection details
 * 
 * @version $Revision: 1.1 $
 */
public class ConnectionKey {
    private String userName;
    private String password;
    private int hash;

    public ConnectionKey(String userName, String password) {
        this.password = password;
        this.userName = userName;
        hash = 31;
        if (userName != null) {
            hash += userName.hashCode();
        }
        hash *= 31;
        if (password != null) {
            hash += password.hashCode();
        }
    }

    public int hashCode() {
        return hash;
    }

    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ConnectionKey) {
            return equals((ConnectionKey)that);
        }
        return false;
    }

    public boolean equals(ConnectionKey that) {
        return isEqual(this.userName, that.userName) && isEqual(this.password, that.password);
    }

    public String getPassword() {
        return password;
    }

    public String getUserName() {
        return userName;
    }

    public static boolean isEqual(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        }
        return o1 != null && o2 != null && o1.equals(o2);
    }

}
