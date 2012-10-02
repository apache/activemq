/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
public class TransactionsDemo {

	public static void main(String[] args) {
		String url = "tcp://localhost:61616";
		String user = null;
		String password = null;
		
		if (args.length >= 1) {
			url = args[0];
		}
		
		if (args.length >= 2) {
			user = args[1];
		}

		if (args.length >= 3) {
			password = args[2];
		}
		
		Retailer r = new Retailer(url, user, password);
		Vendor v = new Vendor(url, user, password);
		Supplier s1 = new Supplier("HardDrive", "StorageOrderQueue", url, user, password);
		Supplier s2 = new Supplier("Monitor", "MonitorOrderQueue", url, user, password);
		
		new Thread(r, "Retailer").start();
		new Thread(v, "Vendor").start();
		new Thread(s1, "Supplier 1").start();
		new Thread(s2, "Supplier 2").start();
	}

}
