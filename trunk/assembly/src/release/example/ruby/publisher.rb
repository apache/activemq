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

@conn = Stomp::Connection.open '', '', 'localhost', 61613, false 
@messages = 10000
@batches = 40
@subscribers = 10
@size = 256

@DATA = "abcdefghijklmnopqrstuvwxyz";
@body = "";
for i in 0..(@size-1)
	@body += @DATA[ i % @DATA.length,1]
end

@times = []
@conn.subscribe '/queue/response', { :ack =>"auto" }

for i in 1..(@batches)
	@body += @DATA[ i % @DATA.length,1]
	sleep 1 if i == 1

 	@start = Time.now	

	for j in 1..@messages
		@conn.send '/topic/event', @body, {'persistent'=>'false'}
		$stdout.print "Sent #{j} messages\n" if j%1000==0
	end
	@conn.send '/topic/event', "REPORT", {'persistent'=>'false'}

	@remaining = @subscribers
	while @remaining > 0
		@msg = @conn.receive
		if @msg.command == "MESSAGE" 
			@remaining -= 1
			$stdout.print "Received report: #{@msg.body}, remaining: #{@remaining}\n"
		else
 			$stdout.print "#{@msg.command}: #{@msg.body}\n"
		end
	end
 	@diff = Time.now-@start

	$stdout.print "Batch #{i} of #{@batches} completed in #{@diff} seconds.\n"
	@times[i] = @diff
end

@conn.send '/topic/event', "SHUTDOWN", {'persistent'=>'false'}

@conn.disconnect