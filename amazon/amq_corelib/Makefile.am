## ---------------------------------------------------------------------------
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
## 
## http://www.apache.org/licenses/LICENSE-2.0
## 
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
## ---------------------------------------------------------------------------

INCLUDES = -I../ -I../marshal
lib_LTLIBRARIES = libamq_corelib.la
libamq_corelib_la_SOURCES = BlockingMessageConsumer.cpp BlockingMessageConsumerRef.cpp CoreLib.cpp CoreLibImpl.cpp ExceptionCallback.cpp Lock.cpp LogLevel.cpp MessageConsumer.cpp MessageConsumerRef.cpp NonBlockingMessageConsumer.cpp NonBlockingMessageConsumerRef.cpp StompMessage.cpp TextMessage.cpp UUIDGenerator.cpp Destination.cpp PrimitiveMap.cpp Sem.cpp
libamq_corelib_la_LIBADD = -L../marshal -lamq_marshal -L../command -lamq_command
include_HEADERS = BlockingMessageConsumer.h BlockingMessageConsumerRef.h Buffer.h BytesMessage.h CoreLib.h CoreLibImpl_.h Destination.h ExceptionCallback.h Exception.h Lock.h Logger.h LogLevel.h MessageConsumer.h MessageConsumerRef.h Message.h NonBlockingMessageConsumer.h NonBlockingMessageConsumerRef.h NullLogger.h RCSID.h StompMessage.h TextMessage.h UUIDGenerator.h PrimitiveMap.h Sem.h

