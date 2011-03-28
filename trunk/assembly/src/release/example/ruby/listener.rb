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
@count = 0

@conn.subscribe '/topic/event', { :ack =>"auto" }
while true 
	@msg = @conn.receive
	if @msg.command == "MESSAGE" 
		if @msg.body == "SHUTDOWN"
			exit 0
	
		elsif @msg.body == "REPORT"
			@diff = Time.now - @start
			@body = "Received #{@count} in #{@diff} seconds";
			@conn.send '/queue/response', @body, {'persistent'=>'false'}
			@count = 0;
		else
			if @count == 0 
				@start = Time.now
			end

			@count += 1;
			if @count % 1000 == 0
 				$stdout.print "Received #{@count} messages.\n"
			end
		end
	else
 		$stdout.print "#{@msg.command}: #{@msg.body}\n"
	end
end
@conn.disconnect