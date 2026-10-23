---
title: "sample report"
slug: /sample-report
---


Performance Test Report Template
--------------------------------
```
<testResult>
  <property name='systemSettings'>
    <props>
      <prop key='java.runtime.name'>Java(TM) 2 Runtime Environment, Standard Edition</prop>
      <prop key='java.vm.version'>1.4.2_10-b03</prop>
                     .
                     .
                     .
      <prop key='os.arch'>x86</prop>
      <prop key='os.name'>Windows XP</prop>
      <prop key='sun.cpu.isalist'>pentium i486 i386</prop>
    </props>
  </property>

  <property name='testProperties'>
    <props>
      <prop key='sysTest.numClients'>5</prop>
                     .
                     .
                     .
      <prop key='sysTest.destDistro'>all</prop>
      <prop key='sysTest.totalDests'>2</prop>
    </props>
  </property>

  <property name='performanceData'>
    <list>
      <value index='0' clientName='TestProducer0'>1035</value>
      <value index='0' clientName='TestProducer3'>255</value>
      <value index='0' clientName='TestProducer4'>727</value>
      <value index='0' clientName='TestProducer2'>347</value>
      <value index='0' clientName='TestProducer1'>527</value>
      <value index='1' clientName='TestProducer0'>947</value>
      <value index='1' clientName='TestProducer3'>615</value>
      <value index='1' clientName='TestProducer4'>712</value>
      <value index='1' clientName='TestProducer2'>864</value>
      <value index='1' clientName='TestProducer1'>187</value>
                     .
                     .
                     .
      <value index='171' clientName='TestProducer0'>1364</value>
      <value index='171' clientName='TestProducer3'>395</value>
      <value index='171' clientName='TestProducer4'>716</value>
      <value index='171' clientName='TestProducer2'>377</value>
      <value index='171' clientName='TestProducer1'>515</value>
    </list>
  </property>

  <property name='perfSummary'>
    <props>
      <prop key='SystemTotalTP'>552114</prop>
      <prop key='SystemTotalClients'>5</prop>
      <prop key='SystemAveTP'>3209.9651162790697</prop>
      <prop key='SystemAveEMMTP'>3161.9651162790697</prop>
      <prop key='SystemAveClientTP'>641.9930232558139</prop>
      <prop key='SystemAveClientEMMTP'>632.3930232558139</prop>
      <prop key='MinClientTP'>clientName=TestProducer4,value=36</prop>
      <prop key='MaxClientTP'>clientName=TestProducer4,value=1764</prop>
      <prop key='MinClientTotalTP'>clientName=TestProducer1,value=107409</prop>
      <prop key='MaxClientTotalTP'>clientName=TestProducer0,value=116751</prop>
      <prop key='MinClientAveTP'>clientName=TestProducer1,value=624.4709302325581</prop>
      <prop key='MaxClientAveTP'>clientName=TestProducer0,value=678.7848837209302</prop>
      <prop key='MinClientAveEMMTP'>clientName=TestProducer1,value=615.2732558139535</prop>
      <prop key='MaxClientAveEMMTP'>clientName=TestProducer0,value=668.9418604651163</prop>
    </props>
  </property>
</testResult>
```
