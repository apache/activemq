#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import sys
import time

from proton import *

user = os.getenv('ACTIVEMQ_USER') or 'admin'
password = os.getenv('ACTIVEMQ_PASSWORD') or 'password'
host = os.getenv('ACTIVEMQ_HOST') or '127.0.0.1'
port = int(os.getenv('ACTIVEMQ_PORT') or 5672)
destination = sys.argv[1:2] or ['topic://event']
destination = destination[0]
address = "amqp://%s@%s:%d/%s"%(user, host, port, destination)

msg = Message()
mng = Messenger()
mng.password=password
mng.start()

messages = 10000

msg.address = address
msg.body = unicode('Hello World from Python')

count = 0
start = time.time()
for _ in xrange(messages):
  mng.put(msg)
  count += 1
  if count % 1000 == 0 :
    print("Sent %d messages"%(count))

msg.body = unicode("SHUTDOWN")
mng.put(msg)
mng.send

diff = time.time() - start
print 'Sent %s frames in %f seconds' % (count, diff)

mng.stop()
