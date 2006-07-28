/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef OW_COMMAND_TYPES_V1_H
#define OW_COMMAND_TYPES_V1_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */
   
   
#define OW_WIREFORMAT_STACK_TRACE_MASK     0x00000001;
#define OW_WIREFORMAT_TCP_NO_DELAY_MASK    0x00000002;
#define OW_WIREFORMAT_CACHE_MASK           0x00000004;
#define OW_WIREFORMAT_COMPRESSION_MASK     0x00000008;   
   
#define OW_NULL_TYPE                       0
#define OW_WIREFORMATINFO_TYPE             1
#define OW_BROKERINFO_TYPE                 2
#define OW_CONNECTIONINFO_TYPE             3
#define OW_SESSIONINFO_TYPE                4
#define OW_CONSUMERINFO_TYPE               5
#define OW_PRODUCERINFO_TYPE               6
#define OW_TRANSACTIONINFO_TYPE            7
#define OW_DESTINATIONINFO_TYPE            8   
#define OW_REMOVESUBSCRIPTIONINFO_TYPE     9
#define OW_KEEPALIVEINFO_TYPE              10
#define OW_SHUTDOWNINFO_TYPE               11
#define OW_REMOVEINFO_TYPE                 12
#define OW_REDELIVERYPOLICY_TYPE           13
#define OW_CONTROLCOMMAND_TYPE             14
#define OW_FLUSHCOMMAND_TYPE               15
#define OW_CONNECTIONERROR_TYPE            16   
#define OW_CONSUMERCONTROL_TYPE            17
#define OW_CONNECTIONCONTROL_TYPE          18
   
#define OW_MESSAGEDISPATCH_TYPE            21
#define OW_MESSAGEACK_TYPE                 22

#define OW_ACTIVEMQMESSAGE_TYPE            23
#define OW_ACTIVEMQBYTESMESSAGE_TYPE       24
#define OW_ACTIVEMQMAPMESSAGE_TYPE         25
#define OW_ACTIVEMQOBJECTMESSAGE_TYPE      26
#define OW_ACTIVEMQSTREAMMESSAGE_TYPE      27
#define OW_ACTIVEMQTEXTMESSAGE_TYPE        28
   
#define OW_RESPONSE_TYPE                   30
#define OW_EXCEPTIONRESPONSE_TYPE          31
#define OW_DATARESPONSE_TYPE               32
#define OW_DATAARRAYRESPONSE_TYPE          33
#define OW_INTEGERRESPONSE_TYPE            34  

#define OW_DISCOVERYEVENT_TYPE             40

#define OW_JOURNALTOPICACK_TYPE            50
#define OW_JOURNALADD_TYPE                 51
#define OW_JOURNALQUEUEACK_TYPE            52
#define OW_JOURNALTRACE_TYPE               53
#define OW_JOURNALTRANSACTION_TYPE         54
#define OW_SUBSCRIPTIONINFO_TYPE           55

#define OW_PARTIALCOMMAND_TYPE             60
#define OW_LASTPARTIALCOMMAND_TYPE         61
#define OW_REPLAYCOMMAND_TYPE              65   
   
#define OW_BYTE_TYPE                       70
#define OW_CHAR_TYPE                       71
#define OW_SHORT_TYPE                      72
#define OW_INTEGER_TYPE                    73
#define OW_LONG_TYPE                       74
#define OW_DOUBLE_TYPE                     75
#define OW_FLOAT_TYPE                      76
#define OW_STRING_TYPE                     77
#define OW_BOOLEAN_TYPE                    78
#define OW_BYTE_ARRAY_TYPE                 79
   
#define OW_MESSAGEDISPATCHNOTIFICATION_TYPE 90
#define OW_NETWORKBRIDGEFILTER_TYPE        91
         
#define OW_ACTIVEMQQUEUE_TYPE              100
#define OW_ACTIVEMQTOPIC_TYPE              101
#define OW_ACTIVEMQTEMPQUEUE_TYPE          102
#define OW_ACTIVEMQTEMPTOPIC_TYPE          103
   
#define OW_MESSAGEID_TYPE                  110
#define OW_LOCALTRANSACTIONID_TYPE         111
#define OW_XATRANSACTIONID_TYPE            112

#define OW_CONNECTIONID_TYPE               120
#define OW_SESSIONID_TYPE                  121
#define OW_CONSUMERID_TYPE                 122
#define OW_PRODUCERID_TYPE                 123
#define OW_BROKERID_TYPE                   124
   
#ifdef __cplusplus
}
#endif

#endif  /* ! OW_COMMAND_TYPES_V1_H */
