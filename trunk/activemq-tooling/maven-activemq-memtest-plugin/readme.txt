####################################################################################################
# Running Maven 2 Memory usage Test
####################################################################################################

Goal                      | Description
--------------------------|----------------------------------------------------------
 activemq-memtest:memtest | Starts the broker, producer, consumer and the memory monitoring thread all in the same VM and
                          | generate the heap and non-heap memory usage of the jvm.
                          | The plugin is included by default in the \activemq-perf module.
                          |
                          | Parameters :
                          |
                          |    1. -DmessageCount - specifies number of messages to send/receive
                          |                      - default value : 100000
                          |
                          |    2. -Dtopic  - specifies domain type. Valid value is true or false
                          |                - default value : true
                          |
                          |    3. -Ddurable - specifies delivery mode: Valid value is true or false
                          |                 - default value : false
                          |
                          |    4. -DconnectionCheckpointSize - specifies size of messages sent in KB before we close and
                          |                                    start the producer/consumer to see if there is a memory
                          |                                    leak using different connections.
                          |                                  - a  value of -1 indicates that no checkpoint is set and will
                          |                                    send/consume messages using one producer/consumer conneciton
                          |                                  - default value : -1
                          |
                          |    5. -DmessageSize - specifies the message size  in bytes
                          |                     - default value : 10240  (10KB)
                          |
                          |    6. -DcheckpointInterval - specifies the interval in seconds on which the monitoring tool
                          |                              will get the memory usage of test run.
                          |                            - default value :  2  (seconds)
                          |
                          |    7. -DprefetchSize - specifies the prefetch size to be used
                          |                      - a value of -1 will indicates that test will use the default prefetch
                          |                        size (32000)
                          |                      - default value : -1
                          |
                          |    8. -Durl - species the broker url to use if not going to be using the embedded broker
                          |             - default value : null
                          |
                          |    9. -DreportName - specifies the name of the output xml file.
                          |                    - default value : activemq-memory-usage-report
                          |
                          |   10. -DreportDirectory - specifies the directory of the output file
                          |                         - default value : ${project.build.directory}/test-memtest
                          |
                          |   11. -DproducerCount - specifies the number of producers
                          |                       - default value : 1
                          |
                          |   12. -DconsumerCount - specifies the number of consumers
                          |                       - default value : 1

-----------------------------------------------------------------------------------------------
|Memory Usage Test sample output
|-----------------------------------------------------------------------------------------------
|<test-report>
|  <test-information>
|    <os-name>Windows XP</os-name>
|    <java-version>1.5.0_05</java-version>
|    <jvm_memory_settings>
|      <heap_memory>
|        <committed>9502720</committed>
|        <max>66650112</max>
|      </heap_memory>
|      <non_heap_memory>
|        <committed>30736384</committed>
|        <max>121634816</max>
|      </non_heap_memory>
|    </jvm_memory_settings>
|    <test-settings>
|      <durable>non-durable</durable>
|      <message_size>10240</message_size>
|      <destination_name>FOO.BAR</destination_name>
|      <connection_checkpoint_size>-1</connection_checkpoint_size>
|      <consumer_count>1</consumer_count>
|      <report_name>activemq-memory-usage-report</report_name>
|      <prefetchSize>-1</prefetchSize>
|      <domain>topic</domain>
|      <producer_count>1</producer_count>
|      <connection_checkpoint_size_kb>-1</connection_checkpoint_size_kb>
|      <message_count>100000</message_count>
|      <report_directory>C:\Projects\logicblaze\activemq\activemq-perftest\target/test-memtest</report_directory>
|    </test-settings>
|  </test-information>
|  <test-result checkpoint_interval_in_sec=5 >
|      <memory_usage index=0 non_heap_mb=21 non_heap_bytes=22963904 heap_mb=6 heap_bytes=7275808/>
|      <memory_usage index=1 non_heap_mb=23 non_heap_bytes=24598560 heap_mb=11 heap_bytes=12474400/>
|      ....
|      ....
|  </test-result>
|</test-report>
|
-------------------------------------------------------------------------------------------------



