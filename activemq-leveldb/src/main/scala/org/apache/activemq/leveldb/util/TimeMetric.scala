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

case class TimeMetric() {
  var max = 0L

  def add(duration:Long) = this.synchronized {
    max = max.max(duration)
  }

  def get = {
    this.synchronized {
      max
    } / 1000000.0
  }
  def reset = {
    this.synchronized {
      val rc = max
      max  = 0
      rc
    } / 1000000.0
  }

  def apply[T](func: =>T):T = {
    val start = System.nanoTime()
    try {
      func
    } finally {
      add(System.nanoTime() - start)
    }
  }

}

