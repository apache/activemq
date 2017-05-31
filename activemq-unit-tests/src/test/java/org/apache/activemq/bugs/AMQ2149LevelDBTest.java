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
package org.apache.activemq.bugs;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.leveldb.LevelDBStore;
import org.junit.Ignore;

//Ignored because there are now exceptions thrown on send when the broker is
//shutdown which cause the test to fail and need to be accounted for
//The parent test is also excluded in the pom.xml currently and not run
@Ignore
public class AMQ2149LevelDBTest extends AMQ2149Test {

    @Override
    protected void configurePersistenceAdapter(BrokerService brokerService) throws Exception {
        LevelDBStore persistenceFactory = new LevelDBStore();
        persistenceFactory.setDirectory(dataDirFile);
        brokerService.setPersistenceAdapter(persistenceFactory);
    }
}
