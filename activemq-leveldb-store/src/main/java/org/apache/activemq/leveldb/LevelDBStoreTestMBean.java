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

package org.apache.activemq.leveldb;

import org.apache.activemq.broker.jmx.MBeanInfo;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface LevelDBStoreTestMBean {

    @MBeanInfo("Used to set if the log force calls should be suspended")
    void setSuspendForce(boolean value);

    @MBeanInfo("Gets if the log force calls should be suspended")
    boolean getSuspendForce();

    @MBeanInfo("Gets the number of threads waiting to do a log force call.")
    long getForceCalls();

    @MBeanInfo("Used to set if the log write calls should be suspended")
    void setSuspendWrite(boolean value);

    @MBeanInfo("Gets if the log write calls should be suspended")
    boolean getSuspendWrite();

    @MBeanInfo("Gets the number of threads waiting to do a log write call.")
    long getWriteCalls();

    @MBeanInfo("Used to set if the log delete calls should be suspended")
    void setSuspendDelete(boolean value);

    @MBeanInfo("Gets if the log delete calls should be suspended")
    boolean getSuspendDelete();

    @MBeanInfo("Gets the number of threads waiting to do a log delete call.")
    long getDeleteCalls();
}
