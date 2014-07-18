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


import collection.mutable.{ListBuffer, HashMap}

import java.io._
import com.fasterxml.jackson.databind.ObjectMapper
import collection.JavaConversions._
import java.util.LinkedHashMap
import java.lang.{IllegalStateException, String}
import beans.BeanProperty
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.zookeeper.KeeperException.NoNodeException
import scala.reflect.ClassTag

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait NodeState {

  /**
   * The id of the cluster node.  There can be multiple node with this ID,
   * but only the first node in the cluster will be the master for for it.
   */
  def id: String

  override
  def toString = new String(ClusteredSupport.encode(this), "UTF-8")
}

class TextNodeState extends NodeState {
  @BeanProperty
  @JsonProperty
  var id:String = _
}

/**
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ClusteredSupport {

  val DEFAULT_MAPPER = new ObjectMapper

  def decode[T](t : Class[T], buffer: Array[Byte], mapper: ObjectMapper=DEFAULT_MAPPER): T = decode(t, new ByteArrayInputStream(buffer), mapper)
  def decode[T](t : Class[T], in: InputStream, mapper: ObjectMapper): T =  mapper.readValue(in, t)

  def encode(value: AnyRef, mapper: ObjectMapper=DEFAULT_MAPPER): Array[Byte] = {
    var baos: ByteArrayOutputStream = new ByteArrayOutputStream
    encode(value, baos, mapper)
    return baos.toByteArray
  }

  def encode(value: AnyRef, out: OutputStream, mapper: ObjectMapper): Unit = {
    mapper.writeValue(out, value)
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClusteredSingletonWatcher[T <: NodeState](val stateClass:Class[T]) extends ChangeListenerSupport {
  import ClusteredSupport._
  
  protected var _group:ZooKeeperGroup = _
  def group = _group

  /**
   * Override to use a custom configured mapper.
   */
  def mapper = ClusteredSupport.DEFAULT_MAPPER

  private val listener = new ChangeListener() {
    def changed() {
      val members = _group.members
      val t = new LinkedHashMap[String, T]()
      members.foreach {
        case (path, data) =>
          try {
            val value = decode(stateClass, data, mapper)
            t.put(path, value)
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
      }
      changed_decoded(t)
    }

    def connected = {
      onConnected
      changed
      ClusteredSingletonWatcher.this.fireConnected
    }

    def disconnected = {
      onDisconnected
      changed
      ClusteredSingletonWatcher.this.fireDisconnected
    }
  }

  protected def onConnected = {}
  protected def onDisconnected = {}

  def start(group:ZooKeeperGroup) = this.synchronized {
    if(_group !=null )
      throw new IllegalStateException("Already started.")
    _group = group
    _group.add(listener)
  }

  def stop = this.synchronized {
    if(_group==null)
      throw new IllegalStateException("Not started.")
    _group.remove(listener)
    _members = HashMap[String, ListBuffer[(String,  T)]]()
    _group = null
  }

  def connected = this.synchronized {
    if(_group==null) {
      false
    } else {
      _group.connected
    }
  }

  protected var _members = HashMap[String, ListBuffer[(String,  T)]]()
  def members = this.synchronized { _members }

  def changed_decoded(m: LinkedHashMap[String, T]) = {
    this.synchronized {
      if( _group!=null ) {
        _members = HashMap[String, ListBuffer[(String,  T)]]()
        m.foreach { case node =>
          _members.getOrElseUpdate(node._2.id, ListBuffer[(String,  T)]()).append(node)
        }
      }
    }
    fireChanged
  }

  def masters = this.synchronized {
    _members.mapValues(_.head._2).toArray.map(_._2).toArray(new ClassTag[T] {
      def runtimeClass = stateClass
      override def erasure = stateClass
    })
  }

}
/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClusteredSingleton[T <: NodeState ](stateClass:Class[T]) extends ClusteredSingletonWatcher[T](stateClass) {
  import ClusteredSupport._

  private var _eid:String = _
  /** the ephemeral id of the node is unique within in the group */
  def eid = _eid
  
  private var _state:T = _

  override def stop = {
    this.synchronized {
      if(_state != null) {
        leave
      }
      super.stop
    }
  }

  def join(state:T):Unit = this.synchronized {
    if(state==null)
      throw new IllegalArgumentException("State cannot be null")
    if(state.id==null)
      throw new IllegalArgumentException("The state id cannot be null")
    if(_group==null)
      throw new IllegalStateException("Not started.")
    this._state = state

    while( connected ) {
      if( _eid == null ) {
        _eid = group.join(encode(state, mapper))
        return;
      } else {
        try {
          _group.update(_eid, encode(state, mapper))
          return;
        } catch {
          case e:NoNodeException =>
            this._eid = null;
        }
      }
    }
  }

  def leave:Unit = this.synchronized {
    if(this._state==null)
      throw new IllegalStateException("Not joined")
    if(_group==null)
      throw new IllegalStateException("Not started.")

    this._state = null.asInstanceOf[T]
    if( _eid!=null && connected ) {
      _group.leave(_eid)
      _eid = null
    }
  }

  override protected def onDisconnected {
  }

  override protected def onConnected {
    if( this._state!=null ) {
      join(this._state)
    }
  }

  def isMaster:Boolean = this.synchronized {
    if(this._state==null)
      return false;
    _members.get(this._state.id) match {
      case Some(nodes) =>
        nodes.headOption.map { x=>
          x._1 == _eid
        }.getOrElse(false)
      case None => false
    }
  }

  def master = this.synchronized {
    if(this._state==null)
      throw new IllegalStateException("Not joined")
    _members.get(this._state.id).map(_.head._2)
  }

  def slaves = this.synchronized {
    if(this._state==null)
      throw new IllegalStateException("Not joined")
    val rc = _members.get(this._state.id).map(_.toList).getOrElse(List())
    rc.drop(1).map(_._2)
  }

}
