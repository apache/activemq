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

user = ENV["ACTIVEMQ_USER"] || "admin"
password = ENV["ACTIVEMQ_PASSWORD"] || "password"
host = ENV["ACTIVEMQ_HOST"] || "localhost"
port = ENV["ACTIVEMQ_PORT"] || 61613
destination = $*[0] || "/topic/event"

begin
  
  $stderr.print "Connecting to stomp://#{host}:#{port} as #{user}\n"
  conn = Stomp::Connection.open user, password, host, port, true
  $stderr.print "Sending input to #{destination}\n"

  headers = {'persistent'=>'false'} 
  headers['reply-to'] = $*[1] if $*[1] != NIL

  STDIN.each_line { |line| 
      conn.publish destination, line, headers
  }
  conn.disconnect

rescue 
end

