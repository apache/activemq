echo off

set SPI_CLASS=org.apache.activemq.tool.spi.JbossMQPojoSPI
set EXT_DIR="c:/jboss-client.jar"
set BROKER_URL_PARAM="factory.brokerUrl=jnp://localhost:1099"
set PROVIDER=JBOSS

REM Config for 1-1-1-Queue-NonPersistent
echo Will run consumer 1-1-1-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_NonPersistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Queue-NonPersistent
echo Will run consumer 10-10-1-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_NonPersistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Queue-NonPersistent
echo Will run consumer 10-10-10-Queue-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_NonPersistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Queue-Persistent
echo Will run consumer 1-1-1-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_Persistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Queue-Persistent
echo Will run consumer 10-10-1-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_Persistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Queue-Persistent
echo Will run consumer 10-10-10-Queue-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-queue
set REPORT_NAME=%PROVIDER%_Cons_Queue_Persistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-NonDurable-NonPersistent
echo Will run consumer 1-1-1-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_NonPersistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-NonDurable-NonPersistent
echo Will run consumer 10-10-1-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_NonPersistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-NonDurable-NonPersistent
echo Will run consumer 10-10-10-Topic-NonDurable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_NonPersistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-NonDurable-Persistent
echo Will run consumer 1-1-1-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_Persistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-NonDurable-Persistent
echo Will run consumer 10-10-1-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_Persistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-NonDurable-Persistent
echo Will run consumer 10-10-10-Topic-NonDurable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-topic-nondurable
set REPORT_NAME=%PROVIDER%_Cons_Topic_NonDurable_Persistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-Durable-NonPersistent
echo Will run consumer 1-1-1-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_NonPersistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-Durable-NonPersistent
echo Will run consumer 10-10-1-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_NonPersistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-Durable-NonPersistent
echo Will run consumer 10-10-10-Topic-Durable-NonPersistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_NonPersistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 1-1-1-Topic-Durable-Persistent
echo Will run consumer 1-1-1-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-1-1-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_Persistent_1_1_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-1-Topic-Durable-Persistent
echo Will run consumer 10-10-1-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-1-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_Persistent_10_10_1.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%

REM Config for 10-10-10-Topic-Durable-Persistent
echo Will run consumer 10-10-10-Topic-Durable-Persistent...
pause
set CONFIG_FILE=./src/main/resources/consumer-conf/AMQ-Cons-10-10-topic-durable
set REPORT_NAME=%PROVIDER%_Cons_Topic_Durable_Persistent_10_10_10.xml
mvn activemq-perf:consumer -DsysTest.propsConfigFile=%CONFIG_FILE% -DsysTest.spiClass=%SPI_CLASS% -Dfactory.extDir=%EXT_DIR% -DsysTest.reportName=%REPORT_NAME% -D%BROKER_URL_PARAM%
