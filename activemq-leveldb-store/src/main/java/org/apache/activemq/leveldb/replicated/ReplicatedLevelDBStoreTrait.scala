package org.apache.activemq.leveldb.replicated

import scala.reflect.BeanProperty
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
