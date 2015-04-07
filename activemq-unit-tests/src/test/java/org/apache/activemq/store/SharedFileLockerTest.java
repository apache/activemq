/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.store;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SharedFileLockerTest
{
   @Rule
   public TemporaryFolder testFolder = new TemporaryFolder();

   @Test
   public void testLock() throws Exception
   {
      final AtomicInteger errors = new AtomicInteger(0);

      Thread thread = null;

      SharedFileLocker locker1 = new SharedFileLocker();
      locker1.setDirectory(testFolder.getRoot());

      final SharedFileLocker locker2 = new SharedFileLocker();
      locker2.setLockAcquireSleepInterval(1);
      locker2.setDirectory(testFolder.getRoot());


      try
      {
         locker1.doStart();

         Assert.assertTrue(locker1.keepAlive());

         Thread.sleep(10);

         thread = new Thread("Locker Thread")
         {
            public void run()
            {
               try
               {
                  locker2.doStart();
               }
               catch (Throwable e)
               {
                  errors.incrementAndGet();
               }
            }
         };

         thread.start();

         // Waiting some small time here, you shouldn't see many messages
         Thread.sleep(100);

         Assert.assertTrue(thread.isAlive());

         locker1.stop();

         // 10 seconds here is an eternity, but it should only take milliseconds
         thread.join(5000);

         long timeout = System.currentTimeMillis() + 5000;

         while (timeout > System.currentTimeMillis() && !locker2.keepAlive())
         {
            Thread.sleep(1);
         }

         Assert.assertTrue(locker2.keepAlive());

         locker2.stop();

      }
      finally
      {
         // to make sure we won't leak threads if the test ever failed for any reason
         thread.join(1000);
         if (thread.isAlive())
         {
            thread.interrupt();
         }

         File lockFile = new File(testFolder.getRoot(), "lock");
         lockFile.delete();
      }

   }
}
