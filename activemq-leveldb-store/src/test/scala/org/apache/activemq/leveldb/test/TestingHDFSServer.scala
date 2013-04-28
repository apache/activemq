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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import java.io.IOException

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object TestingHDFSServer {
  private[leveldb] def start: Unit = {
    var conf: Configuration = new Configuration
    cluster = new MiniDFSCluster(conf, 1, true, null)
    cluster.waitActive
    fs = cluster.getFileSystem
  }

  private[leveldb] def stop: Unit = {
    try {
      cluster.shutdown
    }
    catch {
      case e: Throwable => {
        e.printStackTrace
      }
    }
  }

  private[leveldb] var cluster: MiniDFSCluster = null
  private[leveldb] var fs: FileSystem = null
}