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

import org.apache.activemq.leveldb.replicated.groups._
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.activemq.leveldb.util.{Log, JsonCodec}
import java.io.IOException


class LevelDBNodeState extends NodeState {

  @JsonProperty
  var id: String = _

  @JsonProperty
  var container: String = _

  @JsonProperty
  var address: String = _

  @JsonProperty
  var position: Long = -1

  @JsonProperty
  var weight: Int = 0

  @JsonProperty
  var elected: String = _

  override def equals(obj: Any): Boolean = {
    obj match {
      case x:LevelDBNodeState =>
        x.id == id &&
        x.container == container &&
        x.address == address &&
        x.position == position &&
        x.elected == elected
      case _ => false
    }
  }

  override
  def toString = JsonCodec.encode(this).ascii().toString

}

object MasterElector extends Log

/**
 */
class MasterElector(store: ElectingLevelDBStore) extends ClusteredSingleton[LevelDBNodeState](classOf[LevelDBNodeState]) {
  
  import MasterElector._

  var last_state: LevelDBNodeState = _
  var elected: String = _
  var position: Long = -1
  var address: String = _
  var updating_store = false
  var next_connect: String = _
  var connected_address: String = _

  def join: Unit = this.synchronized {
    last_state = create_state
    join(last_state)
    add(change_listener)
  }

  def elector  = this

  def update: Unit = elector.synchronized {
    var next = create_state
    if (next != last_state) {
      last_state = next
      join(next)
    }
  }

  def create_state = {
    val rc = new LevelDBNodeState
    rc.id = store.brokerName
    rc.elected = elected
    rc.position = position
    rc.weight = store.weight
    rc.address = address
    rc.container = store.container
    rc.address = address
    rc
  }

  object change_listener extends ChangeListener {

    def connected = changed
    def disconnected = {
      changed
    }

    var stopped = false;
    def changed:Unit = elector.synchronized {
      debug("ZooKeeper group changed: %s", members)

//      info(eid+" cluster state changed: "+members)
      if (isMaster) {
        // We are the master elector, we will choose which node will startup the MasterLevelDBStore
        members.get(store.brokerName) match {
          case None =>
            info("Not enough cluster members connected to elect a new master.")
          case Some(members) =>

            if (members.size > store.replicas) {
              warn("Too many cluster members are connected.  Expected at most "+store.replicas+
                      " members but there are "+members.size+" connected.")
            }
            if (members.size < store.clusterSizeQuorum) {
              info("Not enough cluster members connected to elect a master.")
              elected = null
            } else {

              // If we already elected a master, lets make sure he is still online..
              if (elected != null) {
                val by_eid = Map(members: _*)
                if (by_eid.get(elected).isEmpty) {
                  info("Previously elected master is not online, staring new election")
                  elected = null
                }
              }

              // Do we need to elect a new master?
              if (elected == null) {
                // Find the member with the most updates.
                val sortedMembers = members.filter(_._2.position >= 0).sortWith {
                  (a, b) => {
                    a._2.position > b._2.position ||
                      (a._2.position == b._2.position &&  a._2.weight > b._2.weight )
                  }
                }
                if (sortedMembers.size != members.size) {
                  info("Not enough cluster members have reported their update positions yet.")
                } else {
                  // We now have an election.
                  elected = sortedMembers.head._1
                }
              }
              // Sort by the positions in the cluster..
            }
        }
      } else {
        // Only the master sets the elected field.
        elected = null
      }

      val master_elected = if(eid==null) null else master.map(_.elected).getOrElse(null)

      // If no master is currently elected, we need to report our current store position.
      // Since that will be used to select the master.
      val connect_target = if (master_elected != null) {
        position = -1
        members.get(store.brokerName).get.find(_._1 == master_elected).map(_._2.address).getOrElse(null)
      } else {
        // Once we are not running a master or server, report the position..
        if( connected_address==null && address==null && !updating_store ) {
          position = store.position
        }
        null
      }

      // Do we need to stop the running master?
      if ((eid==null || master_elected != eid) && address!=null && !updating_store) {
        info("Demoted to slave")
        updating_store = true
        store.stop_master {
          elector.synchronized {
            updating_store = false
            info("Master stopped")
            address = null
            changed
          }
        }
      }

      // Have we been promoted to being the master?
      if (eid!=null && master_elected == eid && address==null && !updating_store ) {
        info("Promoted to master")
        updating_store = true
        store.start_master { port =>
          elector.synchronized {
            updating_store = false
            address = store.address(port)
            info("Master started: "+address)
            changed
          }
        }
      }

      // Can we become a slave?
      if ( (eid==null || master_elected != eid) && address == null) {
        // Did the master address change?
        if (connect_target != connected_address) {

          // Do we need to setup a new slave.
          if (connect_target != null && !updating_store) {
            updating_store = true
            store.start_slave(connect_target) {
              elector.synchronized {
                updating_store=false
                info("Slave started")
                connected_address = connect_target
                changed
              }
            }
          }

          // Lets stop the slave..
          if (connect_target == null && !updating_store) {
            updating_store = true
            store.stop_slave {
              elector.synchronized {
                updating_store=false
                info("Slave stopped")
                connected_address = null
                changed
              }
            }
          }
        }
      }
      if( group.zk.isConnected ) {
        update
      }
    }
  }
}
