#!/usr/bin/ruby
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

begin

    @port = 61613
    @host = "localhost"
    @user = ENV["STOMP_USER"];
    @password = ENV["STOMP_PASSWORD"]
    
    @host = ENV["STOMP_HOST"] if ENV["STOMP_HOST"] != NIL
    @port = ENV["STOMP_PORT"] if ENV["STOMP_PORT"] != NIL
    
    @destination = "/topic/stompcat"
    @destination = $*[0] if $*[0] != NIL
    
    $stderr.print "Connecting to stomp://#{@host}:#{@port} as #{@user}\n"
    @conn = Stomp::Connection.open @user, @password, @host, @port, true
    $stderr.print "Getting output from #{@destination}\n"

    @conn.subscribe @destination, { :ack =>"client" }
    while true
        @msg = @conn.receive
        $stdout.print @msg.body
        $stdout.flush
        @conn.ack @msg.headers["message-id"]
    end
    @conn.disconnect
  
rescue 
end

