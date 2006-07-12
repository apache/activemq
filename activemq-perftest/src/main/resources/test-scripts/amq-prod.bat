echo off

set SPI_CLASS=org.apache.activemq.tool.spi.ActiveMQReflectionSPI
set EXT_DIR=""
set BROKER_URL_PARAM="factory.brokerURL=tcp://localhost:61616"
set PROVIDER=AMQ

REM Config for 1-1-1-Queue-NonPersistent
echo Will run producer 1-1-1-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-queue-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_NonPersistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Queue-NonPersistent
echo Will run producer 10-10-1-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-queue-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_NonPersistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Queue-NonPersistent
echo Will run producer 10-10-10-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-queue-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_NonPersistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Queue-Persistent
echo Will run producer 1-1-1-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-queue-persistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_Persistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Queue-Persistent
echo Will run producer 10-10-1-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-queue-persistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_Persistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Queue-Persistent
echo Will run producer 10-10-10-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-queue-persistent
set REPORT_NAME=%PROVIDER%_Prod_Queue_Persistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-NonDurable-NonPersistent
echo Will run producer 1-1-1-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_NonPersistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-NonDurable-NonPersistent
echo Will run producer 10-10-1-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_NonPersistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-NonDurable-NonPersistent
echo Will run producer 10-10-10-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_NonPersistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-NonDurable-Persistent
echo Will run producer 1-1-1-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_Persistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-NonDurable-Persistent
echo Will run producer 10-10-1-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_Persistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-NonDurable-Persistent
echo Will run producer 10-10-10-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_NonDurable_Persistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-Durable-NonPersistent
echo Will run producer 1-1-1-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_NonPersistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-Durable-NonPersistent
echo Will run producer 10-10-1-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_NonPersistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-Durable-NonPersistent
echo Will run producer 10-10-10-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-topic-nonpersistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_NonPersistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-Durable-Persistent
echo Will run producer 1-1-1-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-1-1-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_Persistent_1_1_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-Durable-Persistent
echo Will run producer 10-10-1-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-1-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_Persistent_10_10_1.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-Durable-Persistent
echo Will run producer 10-10-10-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/producer-conf/AMQ-Prod-10-10-topic-persistent
set REPORT_NAME=%PROVIDER%_Prod_Topic_Durable_Persistent_10_10_10.xml
mvn activemq-perf:producer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -DsysTest.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%
