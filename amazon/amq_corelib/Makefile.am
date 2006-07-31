INCLUDES = -I../ -I../marshal
lib_LTLIBRARIES = libamq_corelib.la
libamq_corelib_la_SOURCES = BlockingMessageConsumer.cpp BlockingMessageConsumerRef.cpp CoreLib.cpp CoreLibImpl.cpp ExceptionCallback.cpp Lock.cpp LogLevel.cpp MessageConsumer.cpp MessageConsumerRef.cpp NonBlockingMessageConsumer.cpp NonBlockingMessageConsumerRef.cpp StompMessage.cpp TextMessage.cpp UUIDGenerator.cpp Destination.cpp PrimitiveMap.cpp Sem.cpp
libamq_corelib_la_LIBADD = -L../marshal -lamq_marshal -L../command -lamq_command
include_HEADERS = BlockingMessageConsumer.h BlockingMessageConsumerRef.h Buffer.h BytesMessage.h CoreLib.h CoreLibImpl_.h Destination.h ExceptionCallback.h Exception.h Lock.h Logger.h LogLevel.h MessageConsumer.h MessageConsumerRef.h Message.h NonBlockingMessageConsumer.h NonBlockingMessageConsumerRef.h NullLogger.h RCSID.h StompMessage.h TextMessage.h UUIDGenerator.h PrimitiveMap.h Sem.h

