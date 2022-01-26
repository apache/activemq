# Redis persistance store
Implementation of a persistance store using [Redisson](https://github.com/redisson/redisson/wiki/Table-of-Content).

## Motivation
To provide a multi-region persistance for HA and DR purposes. Redis supports [replication](https://redis.io/topics/replication) and is well suited for multi-region architectures. 

## Multi-region failover
A CNAME can be set and changed to point to the active region's address. When a region stops being active and write operations fail the client will resolve CNAME again and reconnect to the newly active region.  

## persistenceAdapter
Not implemented

## jobSchedulerStore

### Loading from redis
Scheduled messages are loaded from redis when broker becomes active.   

### Storing in redis
Scheduled messages are stored into redis as they are scheduled on the broker.

### Expiration
Stored scheduled messages are set to expire at the time they are scheduled for.  
In addition a filter can be set to drop messages that have already expired.  

### Configuration

#### Url format
`schema`://`address`:`port`.   
`schema` is either `redis` for unencrypted connections or `rediss` for TLS.  
`address` is either an `ip address` or a `hostname`.  
`port` is a numerical port.  

#### Mistmatched hostname in certificates
When redis hostname does not match the one in a certificate the connection will fail. To disable hostname validation set `sslEnableEndpointIdentification` to `false`.  

#### Filtering retrieved messages
When scheduler retrieves stored messages it is possible that they have already expired and may need to be filtered out. To achieve that set `filterFactory` to an instance of `org.apache.activemq.store.redis.scheduler.RedisJobSchedulerStoreFilterExpiredFactory`.   

#### Sample broker configuration
```xml
<beans
  xmlns="http://www.springframework.org/schema/beans"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

    <bean id="filterFactory" class="org.apache.activemq.store.redis.scheduler.RedisJobSchedulerStoreFilterExpiredFactory" />

    <broker xmlns="http://activemq.apache.org/schema/core" brokerName="localhost" dataDirectory="${activemq.data}" schedulePeriodForDestinationPurge="10000" schedulerSupport="true">

        <jobSchedulerStore>
            <bean xmlns="http://www.springframework.org/schema/beans" id="jobSchedulerStore" class="org.apache.activemq.store.redis.scheduler.RedisJobSchedulerStore">
                <property name="redisAddress" value="rediss://redis.hostname.here:6379" />
                <property name="redisPassword" value="redis.password.here" />
                <property name="sslEnableEndpointIdentification" value="false" />
                <property name="filterFactory" ref="filterFactory" />
            </bean>
        </jobSchedulerStore>

        ...

    </broker>

</beans>
```

