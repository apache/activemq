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

#include <algorithm>
#include <netinet/in.h>

#include "TextMessage.h"
#include "Buffer.h"
#include "RCSID.h"

using namespace ActiveMQ;
using std::copy;
using std::vector;

RCSID(TextMessage, "$Id$");

TextMessage::TextMessage(const vector<unsigned char>& serializeFrom) {
    text_.assign(serializeFrom.begin() + 4, serializeFrom.end());
}

void
TextMessage::marshall(Buffer& buf) const {
    buf.resize(text_.size() + sizeof(int));
    int size = htonl(text_.size());
    copy((unsigned char *)(&size), ((unsigned char *)(&size)) + sizeof(int), buf.begin());
    copy(text_.begin(), text_.end(), buf.begin()+sizeof(int));
}
