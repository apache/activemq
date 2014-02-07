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
<c:set var="pageTitle" value="Queues"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<div>
<form action="createDestination.action" method="post">
    <input type="hidden" name="JMSDestinationType" value="queue"/>
    <input type="hidden" name="secret" value="<c:out value='${sessionScope["secret"]}'/>"/>

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
<td><a href="<c:url value="browse.jsp">
                        <c:param name="JMSDestination" value="${row.name}" /></c:url>"><form:tooltip text="${row.name}" length="50"/></a></td>
<td><c:out value="${row.queueSize}" /></td>
<td><c:out value="${row.consumerCount}" /></td>
<td><c:out value="${row.enqueueCount}" /></td>
<td><c:out value="${row.dequeueCount}" /></td>
<td>
    <a href="<c:url value="browse.jsp">
                   <c:param name="JMSDestination" value="${row.name}" /></c:url>">Browse</a>
	<a href="<c:url value="queueConsumers.jsp">
	                <c:param name="JMSDestination" value="${row.name}" /></c:url>">Active Consumers</a><br/>
    <a href="<c:url value="queueBrowse/${row.name}">
                    <c:param name="view" value="rss" />
                    <c:param name="feedType" value="atom_1.0" />
                     </c:url>" title="Atom 1.0"><img src="<c:url value="images/feed_atom.png" />" /></a>
    <a href="<c:url value="queueBrowse/${row.name}">
                    <c:param name="view" value="rss" />
                    <c:param name="feedType" value="rss_2.0" />
                    </c:url>" title="RSS 2.0"><img src="<c:url value="images/feed_rss.png" />" /></a>
</td>
<td>
    <a href="<c:url value="send.jsp">
                    <c:param name="JMSDestination" value="${row.name}" />
                    <c:param name="JMSDestinationType" value="queue"/></c:url>">Send To</a>
    <a href="<c:url value="purgeDestination.action">
                    <c:param name="JMSDestination" value="${row.name}" />
                    <c:param name="JMSDestinationType" value="queue"   />
                    <c:param name="secret" value='${sessionScope["secret"]}'/></c:url>">Purge</a>
    <a href="<c:url value="deleteDestination.action">
                    <c:param name="JMSDestination" value="${row.name}" />
                    <c:param name="JMSDestinationType" value="queue"   />
                    <c:param name="secret" value='${sessionScope["secret"]}'/></c:url>">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>

<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
