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
<html>
<head>
<title>Queues</title>
</head>
<body>

<div>
<form action="createDestination.action" method="get">
    <input type="hidden" name="JMSDestinationType" value="queue"/>

    <label name="destination">Queue Name</label>
    <input type="text" name="JMSDestination" value=""/>

    <input type="submit" value="Create"/>
</form>
</div>


<h2>Queues</h2>

<table id="queues" class="sortable autostripe">
<thead>
<tr>
<th>Name</th>
<th>Number Of Pending Messages</th>
<th>Number Of Consumers</th>
<th>Messages Enqueued</th>
<th>Messages Dequeued</th>
<th>Views</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.queues}" var="row">

<tr>
<td><a href="browse.jsp?JMSDestination=<c:out value="${row.name}" />"><form:tooltip text="${row.name}" length="50"/></a></td>
<td>${row.queueSize}</td>
<td>${row.consumerCount}</td>
<td>${row.enqueueCount}</td>
<td>${row.dequeueCount}</td>
<td>
    <a href="browse.jsp?JMSDestination=<c:out value="${row.name}" />">Browse</a>
	<a href="queueConsumers.jsp?JMSDestination=<c:out value="${row.name}" />">Active Consumers</a><br/>
    <a href="queueBrowse/<c:out value="${row.name}" />?view=rss&feedType=atom_1.0" title="Atom 1.0"><img src="images/feed_atom.png"/></a>
    <a href="queueBrowse/<c:out value="${row.name}" />?view=rss&feedType=rss_2.0" title="RSS 2.0"><img src="images/feed_rss.png"/></a>
</td>
<td>
    <a href="send.jsp?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=queue">Send To</a>
    <a href="purgeDestination.action?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=queue">Purge</a>
    <a href="deleteDestination.action?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=queue">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>


</body>
</html>
	
