<%@ page contentType="text/xml;charset=ISO-8859-1"%>
<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
   
    http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<queues>
<c:forEach items="${requestContext.brokerQuery.queues}" var="row">
<queue name="${row.name}">

  <stats size="${row.queueSize}"
         consumerCount="${row.consumerCount}"
         enqueueCount="${row.enqueueCount}"
         dequeueCount="${row.dequeueCount}"/>


  <feed>
    <atom><c:url value="queueBrowse/${row.name}?view=rss&amp;feedType=atom_1.0"/></atom>
    <rss><c:url value="queueBrowse/${row.name}?view=rss&amp;feedType=rss_2.0"/></rss>
  </feed>
</queue>
</c:forEach>
</queues>
