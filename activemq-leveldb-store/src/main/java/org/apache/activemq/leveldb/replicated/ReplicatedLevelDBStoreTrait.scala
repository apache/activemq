package org.apache.activemq.leveldb.replicated

import scala.reflect.BeanProperty
import java.util.UUID
import org.apache.activemq.leveldb.LevelDBStore
import org.apache.activemq.leveldb.util.FileSupport._

/**
 */
trait ReplicatedLevelDBStoreTrait extends LevelDBStore {

  @BeanProperty
  var securityToken = ""

  def replicaId:String = {
    val replicaid_file = directory / "replicaid.txt"
    if( replicaid_file.exists() ) {
      replicaid_file.readText()
    } else {
      val rc = create_uuid
      replicaid_file.getParentFile.mkdirs()
      replicaid_file.writeText(rc)
      rc
    }
  }

  def create_uuid = UUID.randomUUID().toString

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
