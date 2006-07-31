INCLUDES = -I../
lib_LTLIBRARIES = libamq_brokersession.la
libamq_brokersession_la_SOURCES = BrokerSession.cpp BrokerSessionImpl.cpp
libamq_brokersession_la_LIBADD = -L../amq_corelib -lamq_corelib -L../amq_transport -lamq_transport -lpthread
include_HEADERS = BrokerSession.h BrokerSessionImpl_.h
