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
package org.apache.activemq.leveldb.replicated.groups

import internal.ZooKeeperGroup
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.ZooDefs.Ids
import java.util.LinkedHashMap

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroupFactory {

  def create(zk: ZKClient, path: String):Group = new ZooKeeperGroup(zk, path)
  def members(zk: ZKClient, path: String):LinkedHashMap[String, Array[Byte]] = ZooKeeperGroup.members(zk, path)
}

/**
 * <p>
 *   Used the join a cluster group and to monitor the memberships
 *   of that group.
 * </p>
 * <p>
 *   This object is not thread safe.  You should are responsible for
 *   synchronizing access to it across threads.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Group {

  /**
   * Adds a member to the group with some associated data.
   */
  def join(data:Array[Byte]):String

  /**
   * Updates the data associated with joined member.
   */
  def update(id:String, data:Array[Byte]):Unit

  /**
   * Removes a previously added member.
   */
  def leave(id:String):Unit

  /**
   * Lists all the members currently in the group.
   */
  def members:java.util.LinkedHashMap[String, Array[Byte]]

  /**
   * Registers a change listener which will be called
   * when the cluster membership changes.
   */
  def add(listener:ChangeListener)

  /**
   * Removes a previously added change listener.
   */
  def remove(listener:ChangeListener)

  /**
   * A group should be closed to release aquired resources used
   * to monitor the group membership.
   *
   * Whe the Group is closed, any memberships registered via this
   * Group will be removed from the group.
   */
  def close:Unit

  /**
   * Are we connected with the cluster?
   */
  def connected:Boolean
}

/**
 * <p>
 *   Callback interface used to get notifications of changes
 *   to a cluster group.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ChangeListener {
  def changed:Unit
  def connected:Unit
  def disconnected:Unit
}

