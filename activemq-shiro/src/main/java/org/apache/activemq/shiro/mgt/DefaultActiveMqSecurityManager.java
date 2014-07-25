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
package org.apache.activemq.shiro.mgt;

import org.apache.activemq.shiro.session.mgt.DisabledSessionManager;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.DefaultSessionStorageEvaluator;
import org.apache.shiro.mgt.DefaultSubjectDAO;

/**
 * @since 5.10.0
 */
public class DefaultActiveMqSecurityManager extends DefaultSecurityManager {

    public DefaultActiveMqSecurityManager() {
        super();

        //disable sessions entirely:
        setSessionManager(new DisabledSessionManager());

        //also prevent the SecurityManager impl from using the Session as a storage medium (i.e. after authc):
        DefaultSubjectDAO subjectDao = (DefaultSubjectDAO)getSubjectDAO();
        DefaultSessionStorageEvaluator sessionStorageEvaluator =
                (DefaultSessionStorageEvaluator)subjectDao.getSessionStorageEvaluator();
        sessionStorageEvaluator.setSessionStorageEnabled(false);
    }
}
