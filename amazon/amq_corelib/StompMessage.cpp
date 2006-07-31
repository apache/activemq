/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include "StompMessage.h"
#include "Exception.h"
#include "RCSID.h"

using namespace std;
using ActiveMQ::Exception;
using ActiveMQ::StompMessage;

RCSID(StompMessage, "$Id$");

StompMessage::StompMessage(const string& buf)
{
  string::size_type newline = buf.find("\n", 0);
  if (newline == string::npos)
    throw Exception(string("Invalid message: ") + buf);
  if (newline == 0) {
    newline = buf.find("\n", 1);
    if (newline == string::npos)
      throw Exception(string("Invalid message: ") + buf);
    type_ = string(buf, 1, newline - 1);
  }
  else
    type_ = string(buf, 0, newline);
  if (buf.find("\n", newline + 1) == newline + 1) // no headers
    msg_ = string(buf, newline + 1, buf.size() - newline);
  else {
    do {
      string::size_type colon_pos = buf.find(":", newline + 1);
      if (colon_pos == string::npos)
	throw Exception(string("Invalid message: ") + buf);
      string key = string(buf, newline + 1, colon_pos - newline - 1);
      newline = buf.find("\n", colon_pos);
      if (newline == string::npos)
	throw Exception(string("Invalid message: ") + buf);
      headers_[key] = string(buf, colon_pos + 1, newline - colon_pos - 1);
    } while (buf[newline + 1] != '\n');
    msg_ = string(buf, newline + 2, buf.size() - newline - 2);
  }
}
