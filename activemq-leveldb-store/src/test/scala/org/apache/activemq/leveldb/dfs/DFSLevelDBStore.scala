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

package org.apache.activemq.leveldb.dfs

import org.apache.hadoop.conf.Configuration
import org.apache.activemq.util.ServiceStopper
import org.apache.hadoop.fs.FileSystem
import scala.beans.BeanProperty
import java.net.InetAddress
import org.apache.activemq.leveldb.LevelDBStore

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DFSLevelDBStore extends LevelDBStore {

  @BeanProperty
  var dfsUrl:String = _
  @BeanProperty
  var dfsConfig:String = _
  @BeanProperty
  var dfsDirectory:String = _
  @BeanProperty
  var dfsBlockSize = 1024*1024*50L
  @BeanProperty
  var dfsReplication = 1
  @BeanProperty
  var containerId:String = _

  var dfs:FileSystem = _

  override def doStart = {
    if(dfs==null) {
      Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
      val config = new Configuration()
      config.set("fs.hdfs.impl.disable.cache", "true")
      config.set("fs.file.impl.disable.cache", "true")
      Option(dfsConfig).foreach(config.addResource(_))
      Option(dfsUrl).foreach(config.set("fs.default.name", _))
      dfsUrl = config.get("fs.default.name")
      dfs = FileSystem.get(config)
    }
    if ( containerId==null ) {
      containerId = InetAddress.getLocalHost.getHostName
    }
    super.doStart
  }

  override def doStop(stopper: ServiceStopper): Unit = {
    super.doStop(stopper)
    if(dfs!=null){
      dfs.close()
    }
  }

  override def createClient = new DFSLevelDBClient(this)
}