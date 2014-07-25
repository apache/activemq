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

import java.io._
import org.fusesource.hawtdispatch._
import org.apache.activemq.leveldb.LevelDBClient
import org.fusesource.leveldbjni.internal.Util
import org.apache.activemq.leveldb.util.ProcessSupport._
import java.util.zip.CRC32

object FileSupport {

  implicit def toRichFile(file:File):RichFile = new RichFile(file)

  val onWindows = System.getProperty("os.name").toLowerCase().startsWith("windows")
  private var linkStrategy = 0
  private val LOG = Log(getClass)
  
  def link(source:File, target:File):Unit = {
    linkStrategy match {
      case 0 =>
        // We first try to link via a native system call. Fails if
        // we cannot load the JNI module.
        try {
          Util.link(source, target)
        } catch {
          case e:IOException => throw e
          case e:Throwable =>
            // Fallback.. to a slower impl..
            LOG.debug("Native link system call not available")
            linkStrategy = 5
            link(source, target)
        }

      // TODO: consider implementing a case which does the native system call using JNA

      case 5 =>
        // Next we try to do the link by executing an
        // operating system shell command
        try {
          if( onWindows ) {
            system("fsutil", "hardlink", "create", target.getCanonicalPath, source.getCanonicalPath) match {
              case(0, _, _) => // Success
              case (_, out, err) =>
                // TODO: we might want to look at the out/err to see why it failed
                // to avoid falling back to the slower strategy.
                LOG.debug("fsutil OS command not available either")
                linkStrategy = 10
                link(source, target)
            }
          } else {
            system("ln", source.getCanonicalPath, target.getCanonicalPath) match {
              case(0, _, _) => // Success
              case (_, out, err) => None
                // TODO: we might want to look at the out/err to see why it failed
                // to avoid falling back to the slower strategy.
                LOG.debug("ln OS command not available either")
                linkStrategy = 2
                link(source, target)
            }
          }
        } catch {
          case e:Throwable =>
        }
      case _ =>
        // this final strategy is slow but sure to work.
        source.copyTo(target)
    }
  }

  def systemDir(name:String) = {
    val baseValue = System.getProperty(name)
    if( baseValue==null ) {
      sys.error("The the %s system property is not set.".format(name))
    }
    val file = new File(baseValue)
    if( !file.isDirectory  ) {
      sys.error("The the %s system property is not set to valid directory path %s".format(name, baseValue))
    }
    file
  }

  case class RichFile(self:File) {

    def / (path:String) = new File(self, path)

    def linkTo(target:File) = link(self, target)

    def copyTo(target:File) = {
      using(new FileOutputStream(target)){ os=>
        using(new FileInputStream(self)){ is=>
          FileSupport.copy(is, os)
        }
      }
    }

    def crc32(limit:Long=Long.MaxValue) = {
      val checksum =  new CRC32();
      var remaining = limit;
      using(new FileInputStream(self)) { in =>
        val data = new Array[Byte](1024*4)
        var count = in.read(data, 0, remaining.min(data.length).toInt)
        while( count > 0 ) {
          remaining -= count
          checksum.update(data, 0, count);
          count = in.read(data, 0, remaining.min(data.length).toInt)
        }
      }
      checksum.getValue()
    }

    def cached_crc32 = {
      val crc32_file = new File(self.getParentFile, self.getName+".crc32")
      if( crc32_file.exists() && crc32_file.lastModified() > self.lastModified() ) {
        crc32_file.readText().trim.toLong
      } else {
        val rc = crc32()
        crc32_file.writeText(rc.toString)
        rc
      }
    }

    def list_files:Array[File] = {
      Option(self.listFiles()).getOrElse(Array())
    }

    def recursiveList:List[File] = {
      if( self.isDirectory ) {
        self :: self.listFiles.toList.flatten( _.recursiveList )
      } else {
        self :: Nil
      }
    }

    def recursiveDelete: Unit = {
      if( self.exists ) {
        if( self.isDirectory ) {
          self.listFiles.foreach(_.recursiveDelete)
        }
        self.delete
      }
    }

    def recursiveCopyTo(target: File) : Unit = {
      if (self.isDirectory) {
        target.mkdirs
        self.listFiles.foreach( file=> file.recursiveCopyTo( target / file.getName) )
      } else {
        self.copyTo(target)
      }
    }

    def readText(charset:String="UTF-8"): String = {
      using(new FileInputStream(self)) { in =>
        FileSupport.readText(in, charset)
      }
    }

    def readBytes: Array[Byte] = {
      using(new FileInputStream(self)) { in =>
        FileSupport.readBytes(in)
      }
    }

    def writeBytes(data:Array[Byte]):Unit = {
      using(new FileOutputStream(self)) { out =>
        FileSupport.writeBytes(out, data)
      }
    }

    def writeText(data:String, charset:String="UTF-8"):Unit = {
      using(new FileOutputStream(self)) { out =>
        FileSupport.writeText(out, data, charset)
      }
    }

  }

  /**
   * Returns the number of bytes copied.
   */
  def copy(in: InputStream, out: OutputStream): Long = {
    var bytesCopied: Long = 0
    val buffer = new Array[Byte](8192)
    var bytes = in.read(buffer)
    while (bytes >= 0) {
      out.write(buffer, 0, bytes)
      bytesCopied += bytes
      bytes = in.read(buffer)
    }
    bytesCopied
  }

  def using[R,C <: Closeable](closable: C)(proc: C=>R) = {
    try {
      proc(closable)
    } finally {
      try { closable.close  }  catch { case ignore:Throwable =>  }
    }
  }

  def readText(in: InputStream, charset:String="UTF-8"): String = {
    new String(readBytes(in), charset)
  }

  def readBytes(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    copy(in, out)
    out.toByteArray
  }

  def writeText(out: OutputStream, value: String, charset:String="UTF-8"): Unit = {
    writeBytes(out, value.getBytes(charset))
  }

  def writeBytes(out: OutputStream, data: Array[Byte]): Unit = {
    copy(new ByteArrayInputStream(data), out)
  }

}

object ProcessSupport {
  import FileSupport._

  implicit def toRichProcessBuilder(self:ProcessBuilder):RichProcessBuilder = new RichProcessBuilder(self)

  case class RichProcessBuilder(self:ProcessBuilder) {

    def start(out:OutputStream=null, err:OutputStream=null, in:InputStream=null) = {
      self.redirectErrorStream(out == err)
      val process = self.start
      if( in!=null ) {
        LevelDBClient.THREAD_POOL {
          try {
            using(process.getOutputStream) { out =>
              FileSupport.copy(in, out)
            }
          } catch {
            case _ : Throwable =>
          }
        }
      } else {
        process.getOutputStream.close
      }

      if( out!=null ) {
        LevelDBClient.THREAD_POOL {
          try {
            using(process.getInputStream) { in =>
              FileSupport.copy(in, out)
            }
          } catch {
            case _ : Throwable =>
          }
        }
      } else {
        process.getInputStream.close
      }

      if( err!=null && err!=out ) {
        LevelDBClient.THREAD_POOL {
          try {
            using(process.getErrorStream) { in =>
              FileSupport.copy(in, err)
            }
          } catch {
            case _ : Throwable =>
          }
        }
      } else {
        process.getErrorStream.close
      }
      process
    }

  }

  implicit def toRichProcess(self:Process):RichProcess = new RichProcess(self)

  case class RichProcess(self:Process) {
    def onExit(func: (Int)=>Unit) = LevelDBClient.THREAD_POOL {
      self.waitFor
      func(self.exitValue)
    }
  }

  implicit def toProcessBuilder(args:Seq[String]):ProcessBuilder = new ProcessBuilder().command(args : _*)

  def launch(command:String*)(func: (Int, Array[Byte], Array[Byte])=>Unit ):Unit = launch(command)(func)
  def launch(p:ProcessBuilder, in:InputStream=null)(func: (Int, Array[Byte], Array[Byte]) => Unit):Unit = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    p.start(out, err, in).onExit { code=>
      func(code, out.toByteArray, err.toByteArray)
    }
  }

  def system(command:String*):(Int, Array[Byte], Array[Byte]) = system(command)
  def system(p:ProcessBuilder, in:InputStream=null):(Int, Array[Byte], Array[Byte]) = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    val process = p.start(out, err, in)
    process.waitFor
    (process.exitValue, out.toByteArray, err.toByteArray)
  }

}