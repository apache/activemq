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
package org.apache.activemq.leveldb.test

import org.apache.hadoop.fs.FileUtil
import java.io.File
import java.util.concurrent.TimeUnit
import org.apache.activemq.leveldb.{LevelDBStore}
import org.apache.activemq.leveldb.dfs.DFSLevelDBStore

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DFSLevelDBFastEnqueueTest extends LevelDBFastEnqueueTest {

  override def setUp: Unit = {
    TestingHDFSServer.start
    super.setUp
  }

  override def tearDown: Unit = {
    super.tearDown
    TestingHDFSServer.stop
  }

  override protected def createStore: LevelDBStore = {
    var store: DFSLevelDBStore = new DFSLevelDBStore
    store.setDirectory(dataDirectory)
    store.setDfsDirectory("target/activemq-data/hdfs-leveldb")
    return store
  }

  private def dataDirectory: File = {
    return new File("target/activemq-data/leveldb")
  }

  /**
   * On restart we will also delete the local file system store, so that we test restoring from
   * HDFS.
   */
  override  protected def restartBroker(restartDelay: Int, checkpoint: Int): Unit = {
    stopBroker
    FileUtil.fullyDelete(dataDirectory)
    TimeUnit.MILLISECONDS.sleep(restartDelay)
    startBroker(false, checkpoint)
  }
}