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

import org.apache.zookeeper._
import org.linkedin.zookeeper.tracker._
import scala.collection.mutable.HashMap
import org.linkedin.zookeeper.client.LifecycleListener
import collection.JavaConversions._
import java.util.{LinkedHashMap, Collection}
import org.apache.zookeeper.KeeperException.{ConnectionLossException, NoNodeException}
import scala.Predef._
import scala.Some


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroupFactory {

  def create(zk: ZKClient, path: String):ZooKeeperGroup = new ZooKeeperGroup(zk, path)
  def members(zk: ZKClient, path: String):LinkedHashMap[String, Array[Byte]] = ZooKeeperGroup.members(zk, path)
}


/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZooKeeperGroup {
  def members(zk: ZKClient, path: String):LinkedHashMap[String, Array[Byte]] = {
    var rc = new LinkedHashMap[String, Array[Byte]]
    zk.getAllChildren(path).sortWith((a,b)=> a < b).foreach { node =>
      try {
        if( node.matches("""0\d+""") ) {
          rc.put(node, zk.getData(path+"/"+node))
        } else {
          None
        }
      } catch {
        case e:Throwable =>
          e.printStackTrace
      }
    }
    rc

  }


}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ZooKeeperGroup(val zk: ZKClient, val root: String) extends LifecycleListener with ChangeListenerSupport {

  var tree = new ZooKeeperTreeTracker[Array[Byte]](zk, new ZKByteArrayDataReader, root, 1)
  var rebuildTree = false

  val joins = HashMap[String, Int]()

  var members = new LinkedHashMap[String, Array[Byte]]

  private def member_path_prefix = root + "/0"

  zk.registerListener(this)

  create(root)
  var treeEventHandler = new NodeEventsListener[Array[Byte]]() {
    def onEvents(events: Collection[NodeEvent[Array[Byte]]]): Unit = {
      if( !closed )
        fire_cluster_change;
    }
  }
  tree.track(treeEventHandler)
  fire_cluster_change

  @volatile
  var closed = false

  def close = this.synchronized {
    closed = true
    joins.foreach { case (path, version) =>
      try {
        if( zk.isConnected ) {
          zk.delete(member_path_prefix + path, version)
        }
      } catch {
        case x:NoNodeException => // Already deleted.
      }
    }
    joins.clear
    tree.destroy
    zk.removeListener(this)
  }

  def connected = zk.isConnected
  def onConnected() = {
    this.synchronized {
      // underlying ZooKeeperTreeTracker isn't rebuilding itself after
      // the loss of the session, so we need to destroy/rebuild it on
      // reconnect.
      if (rebuildTree) {
        tree.destroy
        tree = new ZooKeeperTreeTracker[Array[Byte]](zk, new ZKByteArrayDataReader, root, 1)
        tree.track(treeEventHandler)
      } else {
        rebuildTree = true
      }
    }
    fireConnected()
  }
  def onDisconnected() = {
    this.members = new LinkedHashMap()
    fireDisconnected()
  }

  def join(data:Array[Byte]=null): String = this.synchronized {
    val id = zk.createWithParents(member_path_prefix, data, CreateMode.EPHEMERAL_SEQUENTIAL).stripPrefix(member_path_prefix)
    joins.put(id, 0)
    id
  }

  def update(path:String, data:Array[Byte]=null): Unit = this.synchronized {
    joins.get(path) match {
      case Some(ver) =>
        try {
          val stat = zk.setData(member_path_prefix + path, data, ver)
          joins.put(path, stat.getVersion)
        }
        catch {
          case e:NoNodeException =>
            joins.remove(path)
            throw e;
        }
      case None => throw new NoNodeException("Has not joined locally: "+path)
    }
  }

  def leave(path:String): Unit = this.synchronized {
    joins.remove(path).foreach {
      case version =>
          try {
            zk.delete(member_path_prefix + path, version)
          } catch {
            case x: NoNodeException => // Already deleted.
            case x: ConnectionLossException => // disconnected
          }
    }
  }

  private def fire_cluster_change: Unit = {
    this.synchronized {
      val t = tree.getTree.toList.filterNot { x =>
      // don't include the root node, or nodes that don't match our naming convention.
        (x._1 == root) || !x._1.stripPrefix(root).matches("""/0\d+""")
      }

      this.members = new LinkedHashMap()
      t.sortWith((a,b)=> a._1 < b._1 ).foreach { x=>
        this.members.put(x._1.stripPrefix(member_path_prefix), x._2.getData)
      }
    }
    fireChanged()
  }

  private def create(path: String, count : java.lang.Integer = 0): Unit = {
    try {
      if (zk.exists(path, false) != null) {
        return
      }
      try {
        // try create given path in persistent mode
        zk.createOrSetWithParents(path, "", CreateMode.PERSISTENT)
      } catch {
        case ignore: KeeperException.NodeExistsException =>
      }
    } catch {
      case ignore : KeeperException.SessionExpiredException => {
        if (count > 20) {
          // we tried enought number of times
          throw new IllegalStateException("Cannot create path " + path, ignore)
        }
        // try to create path with increased counter value
        create(path, count + 1)
      }
    }
  }
}
