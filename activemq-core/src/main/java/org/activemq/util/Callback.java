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
package org.activemq.util;

/**
 * A simple callback object used by the
 * {@link org.activemq.util.TransactionTemplate}
 * and {@link org.activemq.util.ExceptionTemplate}
   objects to provide automatic transactional or exception handling blocks.
 *
 * @version $Revision: 1.2 $
 */
public interface Callback {

    /**
     * Executes some piece of code within a transaction
     * performing a commit if there is no exception thrown
     * else a rollback is performed
     *
     * @throws Throwable
     */
    public void execute() throws Throwable;
}
