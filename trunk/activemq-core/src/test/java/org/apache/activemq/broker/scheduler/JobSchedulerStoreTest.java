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
package org.apache.activemq.broker.scheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.activemq.util.IOHelper;
import org.apache.kahadb.util.ByteSequence;

public class JobSchedulerStoreTest extends TestCase {

	public void testRestart() throws Exception {
		JobSchedulerStore store = new JobSchedulerStore();
		File directory = new File("target/test/ScheduledDB");
		  IOHelper.mkdirs(directory);
	      IOHelper.deleteChildren(directory);
	      store.setDirectory(directory);
		final int NUMBER = 1000;
		store.start();
		List<ByteSequence>list = new ArrayList<ByteSequence>();
		for (int i = 0; i < NUMBER;i++ ) {
            ByteSequence buff = new ByteSequence(new String("testjob"+i).getBytes());
            list.add(buff);     
        }
		JobScheduler js = store.getJobScheduler("test");
		int count = 0;
		long startTime = 10000;
		for (ByteSequence job:list) {
		    js.schedule("id:"+(count++), job,"",startTime,10000,-1);	    
		}
		List<Job>test = js.getAllJobs();
		assertEquals(list.size(),test.size());
		store.stop();
		
		store.start();
		js = store.getJobScheduler("test");
		test = js.getAllJobs();
		assertEquals(list.size(),test.size());
		for (int i = 0; i < list.size();i++) {
		    String orig = new String(list.get(i).getData());
		    String payload = new String(test.get(i).getPayload());
		    assertEquals(orig,payload);
		}
	}
}
