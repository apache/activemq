---
title: "JMS and JDBC operations in one transaction"
description: "Spring example running JMS and JDBC work in one XA transaction with ActiveMQ, Jencks, the Geronimo transaction manager and an Enhydra PostgreSQL data source."
slug: /jms-and-jdbc-operations-in-one-transaction
---


### JMS and JDBC operations in one transaction with Spring/Jencks/ActiveMQ

Spring beans:
```
<beans>
    <!-- ActiveMQ Broker -->
    <bean id="broker" class="org.apache.activemq.broker.BrokerService" init-method="start" destroy-method="stop">
        <property name="persistent" value="false"/>
        <property name="transportConnectorURIs">
            <list>
                <value>tcp://localhost:5000</value>
            </list>
        </property>
    </bean>

    <!-- Geronimo Transaction Manager -->
    <bean id="transactionContextManager" class="org.jencks.factory.TransactionContextManagerFactoryBean"/>
    <bean id="geronimo" class="org.jencks.factory.GeronimoTransactionManagerFactoryBean"/>
    <bean id="geronimoTransactionManager" class="org.springframework.transaction.jta.JtaTransactionManager">
        <property name="userTransaction" ref="geronimo" />
    </bean>

    <!-- Jencks Connection Manager -->
    <bean id="connectionManager" class="org.jencks.factory.ConnectionManagerFactoryBean">
        <property name="transactionSupport">
            <bean class="org.jencks.factory.XATransactionFactoryBean">
                <property name="useTransactionCaching" value="true"/>
                <property name="useThreadCaching" value="false"/>
            </bean>
        </property>
        <property name="poolingSupport">
            <bean class="org.jencks.factory.SinglePoolFactoryBean">
                <property name="maxSize" value="2"/>
                <property name="minSize" value="1"/>
                <property name="blockingTimeoutMilliseconds" value="60"/>
                <property name="idleTimeoutMinutes" value="60"/>
                <property name="matchOne" value="true"/>
                <property name="matchAll" value="true"/>
                <property name="selectOneAssumeMatch" value="true"/>
            </bean>
        </property>
    </bean>

    <!-- ActiveMQ Connection -->
    <bean id="jmsResourceAdapter" class="org.apache.activemq.ra.ActiveMQResourceAdapter" depends-on="broker">
        <property name="serverUrl">
            <value>tcp://localhost:5000</value>
        </property>
    </bean>
    <bean id="jmsManagedConnectionFactory" class="org.apache.activemq.ra.ActiveMQManagedConnectionFactory">
        <property name="resourceAdapter" ref="jmsResourceAdapter"/>
    </bean>
    <bean id="jmsConnectionFactory" class="org.springframework.jca.support.LocalConnectionFactoryBean">
        <property name="managedConnectionFactory" ref="jmsManagedConnectionFactory"/>
        <property name="connectionManager" ref="connectionManager"/>
    </bean>

    <!-- Tranql JDBC Connection -->
    <!--
    <bean id="tranqlManagedConnectionFactory" class="org.jencks.tranql.XAPoolDataSourceMCF">
        <property name="driverName" value="org.postgresql.Driver"/>
        <property name="url" value="jdbc:postgresql://ats-manager/activemq"/>
        <property name="user" value="activemq"/>
    </bean>
    <bean id="tranqlDataSource" class="org.springframework.jca.support.LocalConnectionFactoryBean">
        <property name="managedConnectionFactory" ref="tranqlManagedConnectionFactory"/>
        <property name="connectionManager" ref="connectionManager"/>
    </bean>
    -->

    <!-- Enhydra JDBC Connection -->
    <bean id="enhydraDataSource" class="org.enhydra.jdbc.pool.StandardXAPoolDataSource" destroy-method="shutdown">
        <property name="dataSource">
            <bean class="org.enhydra.jdbc.standard.StandardXADataSource" destroy-method="shutdown">
                <property name="transactionManager" ref="geronimo" />
                <property name="driverName" value="org.postgresql.Driver" />
                <property name="url" value="jdbc:postgresql://ats-manager/activemq" />
            </bean>
        </property>
        <property name="user" value="activemq"/>
    </bean>

    <bean id="jencksJCAContainer" class="org.jencks.JCAContainer">
        <property name="bootstrapContext">
            <bean class="org.jencks.factory.BootstrapContextFactoryBean">
                <property name="threadPoolSize" value="25"/>
            </bean>
        </property>
        <property name="resourceAdapter" ref="jmsResourceAdapter"/>
    </bean>

    <bean id="inboundConnector" class="org.jencks.JCAConnector">
        <property name="jcaContainer" ref="jencksJCAContainer" />
        <property name="activationSpec">
            <bean class="org.apache.activemq.ra.ActiveMQActivationSpec">
                <property name="destination" value="messages.input"/>
                <property name="destinationType" value="javax.jms.Queue"/>
            </bean>
        </property>
        <property name="transactionManager" ref="geronimo"/>
        <property name="ref" value="echoBean"/>
    </bean>

    <bean id="echoBean" class="transactions.EchoBean">
        <property name="jdbcTemplate">
            <bean class="org.springframework.jdbc.core.JdbcTemplate">
                <property name="dataSource" ref="enhydraDataSource"/>
            </bean>
        </property>
        <property name="jmsTemplate">
            <bean class="org.springframework.jms.core.JmsTemplate">
                <property name="connectionFactory" ref="jmsConnectionFactory"/>
            </bean>
        </property>
    </bean>
</beans>
```
Java code:
```
public class EchoBean implements MessageListener {

    private Log log = LogFactory.getLog(getClass());

    private JdbcTemplate jdbcTemplate;
    private JmsTemplate jmsTemplate;

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setJmsTemplate(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public void onMessage(Message message) {
        log.debug(message);
        if (message instanceof TextMessage) {
            try {
                String messageText = ((TextMessage)message).getText();
                log.debug("execute JMS operation");
                jmsTemplate.convertAndSend("messages.ouptut", messageText);
                log.debug("execute JDBC operation");
                jdbcTemplate.execute("insert into t1 values('"+messageText+"')");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
```
