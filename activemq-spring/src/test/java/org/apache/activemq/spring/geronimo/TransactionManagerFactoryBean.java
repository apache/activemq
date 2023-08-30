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
package org.apache.activemq.spring.geronimo;

import java.util.Collection;

import org.apache.geronimo.transaction.log.UnrecoverableLog;
import org.apache.geronimo.transaction.log.HOWLLog;
import org.apache.geronimo.transaction.manager.TransactionLog;
import org.apache.geronimo.transaction.manager.XidFactory;
import org.apache.geronimo.transaction.manager.XidFactoryImpl;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.DisposableBean;

/**
 * This FactoryBean creates and configures the Geronimo implementation
 * of the TransactionManager interface.
 *
 * @author Thierry Templier
 * @see UnrecoverableLog
 * @see org.apache.geronimo.transaction.log.HOWLLog
 * @org.apache.xbean.XBean element="transactionManager"
 */
public class TransactionManagerFactoryBean implements FactoryBean, InitializingBean, DisposableBean {
    private GeronimoPlatformTransactionManager transactionManager;

    private int defaultTransactionTimeoutSeconds = 600;
    private XidFactory xidFactory;

    private TransactionLog transactionLog;
    private String transactionLogDir;

    private boolean createdTransactionLog;


    public Object getObject() throws Exception {
        if (transactionManager == null) {
            this.transactionManager = new GeronimoPlatformTransactionManager(
                    defaultTransactionTimeoutSeconds,
                    xidFactory,
                    transactionLog);
        }
        return transactionManager;
    }

    public void destroy() throws Exception {
        if (createdTransactionLog && transactionLog instanceof HOWLLog) {
            ((HOWLLog)transactionLog).doStop();
        }
    }

    public Class<?> getObjectType() {
        return GeronimoPlatformTransactionManager.class;
    }

    public boolean isSingleton() {
        return true;
    }

    /**
     * Set the default transaction timeout in second.
     */
    public void setDefaultTransactionTimeoutSeconds(int timeout) {
        defaultTransactionTimeoutSeconds = timeout;
    }

    /**
     * Set the transaction log for the transaction context manager.
     */
    public void setTransactionLog(TransactionLog log) {
        transactionLog = log;
    }

    public String getTransactionLogDir() {
        return transactionLogDir;
    }

    public void setTransactionLogDir(String transactionLogDir) {
        this.transactionLogDir = transactionLogDir;
    }

    public XidFactory getXidFactory() {
        return xidFactory;
    }

    public void setXidFactory(XidFactory xidFactory) {
        this.xidFactory = xidFactory;
    }

    /**
     * Set the resource managers
     */
    public void setResourceManagers(Collection<?> resourceManagers) {
        // TODO: warn about deprecated method
    }

    public void afterPropertiesSet() throws Exception {
        if (transactionLog == null) {
            transactionLog = createTransactionLog(xidFactory, transactionLogDir);
            createdTransactionLog = true;
        }
        if (xidFactory == null) {
            xidFactory = new XidFactoryImpl();
        }
    }

    public static TransactionLog createTransactionLog(XidFactory xidFactory, String logDir) throws Exception {
        if (logDir == null) {
            return new UnrecoverableLog();
        } else {
            HowlLogFactoryBean howlLogFactoryBean = new HowlLogFactoryBean();
            howlLogFactoryBean.setXidFactory(xidFactory);
            howlLogFactoryBean.setLogFileDir(logDir);
            return (TransactionLog) howlLogFactoryBean.getObject();
        }
    }
}
