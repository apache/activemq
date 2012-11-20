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

package org.apache.activemq.leveldb.util

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object RetrySupport {

  def retry[T](log:Log, isStarted: ()=>Boolean, func: ()=>T): T = {
    import log._
    var error:Throwable = null
    var rc:Option[T] = None

    // We will loop until the tx succeeds.  Perhaps it's
    // failing due to a temporary condition like low disk space.
    while(!rc.isDefined) {

      try {
        rc = Some(func())
      } catch {
        case e:Throwable =>
          if( error==null ) {
            warn(e, "DB operation failed. (entering recovery mode)")
          }
          error = e
      }

      if (!rc.isDefined) {
        // We may need to give up if the store is being stopped.
        if ( !isStarted() ) {
          throw error
        }
        Thread.sleep(1000)
      }
    }

    if( error!=null ) {
      info("DB recovered from failure.")
    }
    rc.get
  }

}