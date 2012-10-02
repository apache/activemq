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

import java.util.concurrent.atomic.AtomicLong
import org.slf4j.{MDC, Logger, LoggerFactory}
import java.lang.{Throwable, String}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Log {

  def apply(clazz:Class[_]):Log = apply(clazz.getName.stripSuffix("$"))

  def apply(name:String):Log = new Log {
    override val log = LoggerFactory.getLogger(name)
  }

  def apply(value:Logger):Log = new Log {
    override val log = value
  }

  val exception_id_generator = new AtomicLong(System.currentTimeMillis)
  def next_exception_id = exception_id_generator.incrementAndGet.toHexString
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Log {
  import Log._
  val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  private def with_throwable(e:Throwable)(func: =>Unit) = {
    if( e!=null ) {
      val stack_ref = if( log.isDebugEnabled ) {
        val id = next_exception_id
        MDC.put("stackref", id.toString);
        Some(id)
      } else {
        None
      }
      func
      stack_ref.foreach { id=>
        log.debug(e.toString, e)
        MDC.remove("stackref")
      }
    } else {
      func
    }
  }

  private def format(message:String, args:Seq[Any]) = {
    if( args.isEmpty ) {
      message
    } else {
      message.format(args.map(_.asInstanceOf[AnyRef]) : _*)
    }
  }

  def error(m: => String, args:Any*): Unit = {
    if( log.isErrorEnabled ) {
      log.error(format(m, args.toSeq))
    }
  }

  def error(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isErrorEnabled ) {
        log.error(format(m, args.toSeq))
      }
    }
  }

  def error(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isErrorEnabled ) {
        log.error(e.getMessage)
      }
    }
  }

  def warn(m: => String, args:Any*): Unit = {
    if( log.isWarnEnabled ) {
      log.warn(format(m, args.toSeq))
    }
  }

  def warn(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isWarnEnabled ) {
        log.warn(format(m, args.toSeq))
      }
    }
  }

  def warn(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isWarnEnabled ) {
        log.warn(e.toString)
      }
    }
  }

  def info(m: => String, args:Any*): Unit = {
    if( log.isInfoEnabled ) {
      log.info(format(m, args.toSeq))
    }
  }

  def info(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isInfoEnabled ) {
        log.info(format(m, args.toSeq))
      }
    }
  }

  def info(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isInfoEnabled ) {
        log.info(e.toString)
      }
    }
  }


  def debug(m: => String, args:Any*): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(format(m, args.toSeq))
    }
  }

  def debug(e: Throwable, m: => String, args:Any*): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(format(m, args.toSeq), e)
    }
  }

  def debug(e: Throwable): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(e.toString, e)
    }
  }

  def trace(m: => String, args:Any*): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(format(m, args.toSeq))
    }
  }

  def trace(e: Throwable, m: => String, args:Any*): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(format(m, args.toSeq), e)
    }
  }

  def trace(e: Throwable): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(e.toString, e)
    }
  }

}