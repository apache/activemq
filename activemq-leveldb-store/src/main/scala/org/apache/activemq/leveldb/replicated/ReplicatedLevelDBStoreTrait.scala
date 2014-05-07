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
package org.apache.activemq.leveldb.replicated

import scala.beans.BeanProperty
import java.util.UUID
import org.apache.activemq.leveldb.LevelDBStore
import org.apache.activemq.leveldb.util.FileSupport._
import java.io.File

object ReplicatedLevelDBStoreTrait {

  def create_uuid = UUID.randomUUID().toString

  def node_id(directory:File):String = {
    val nodeid_file = directory / "nodeid.txt"
    if( nodeid_file.exists() ) {
      nodeid_file.readText()
    } else {
      val rc = create_uuid
      nodeid_file.getParentFile.mkdirs()
      nodeid_file.writeText(rc)
      rc
    }
  }
}

/**
 */
trait ReplicatedLevelDBStoreTrait extends LevelDBStore {

  @BeanProperty
  var securityToken = ""

  def node_id = ReplicatedLevelDBStoreTrait.node_id(directory)

  def storeId:String = {
    val storeid_file = directory / "storeid.txt"
    if( storeid_file.exists() ) {
      storeid_file.readText()
    } else {
      null
    }
  }

  def storeId_=(value:String) {
    val storeid_file = directory / "storeid.txt"
    storeid_file.writeText(value)
  }


}
