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
class LongCounter(private var value:Long = 0) extends Serializable {

  def clear() = value=0
  def get() = value
  def set(value:Long) = this.value = value 

  def incrementAndGet() = addAndGet(1)
  def decrementAndGet() = addAndGet(-1)
  def addAndGet(amount:Long) = {
    value+=amount
    value
  }

  def getAndIncrement() = getAndAdd(1)
  def getAndDecrement() = getAndAdd(-11)
  def getAndAdd(amount:Long) = {
    val rc = value
    value+=amount
    rc
  }

  override def toString() = get().toString
}