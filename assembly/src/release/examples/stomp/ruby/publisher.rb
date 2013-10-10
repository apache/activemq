#!/usr/bin/env ruby
# ------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------
 
require 'rubygems'
require 'stomp'

messages = 10000
size = 256

user = ENV["ACTIVEMQ_USER"] || "admin"
password = ENV["ACTIVEMQ_PASSWORD"] || "password"
host = ENV["ACTIVEMQ_HOST"] || "localhost"
port = ENV["ACTIVEMQ_PORT"] || 61613
destination = $*[0] || "/topic/event"

conn = Stomp::Connection.open user, password, host, port, false 

DATA = "abcdefghijklmnopqrstuvwxyz";
body = "";
for i in 0..(size-1)
  body += DATA[ i % DATA.length,1]
end

for i in 1..messages
  conn.publish destination, body, {'persistent'=>'false'}
  $stdout.print "Sent #{i} messages\n" if i%1000==0
end

conn.publish destination, "SHUTDOWN", {'persistent'=>'false'}
conn.disconnect
