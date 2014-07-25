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
<c:set var="pageTitle" value="Topics"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<div>
<form action="createDestination.action" method="post">
    <input type="hidden" name="JMSDestinationType" value="topic"/>
    <input type="hidden" name="secret" value="<c:out value='${sessionScope["secret"]}'/>"/>

    <label name="destination">Topic Name</label>
    <input type="text" name="JMSDestination" value=""/>

    <input type="submit" value="Create"/>
</form>
</div>

<h2>Topics</h2>

<table id="topics" class="sortable autostripe">
<thead>
<tr>
<th>Name</th>
<th>Number Of Consumers</th>
<th>Messages Enqueued</th>
<th>Messages Dequeued</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.topics}" var="row">
<tr>
<td><a href="<c:url value="send.jsp">
                        <c:param name="JMSDestination" value="${row.name}" />
                        <c:param name="JMSDestinationType" value="topic"/></c:url>"><form:tooltip text="${row.name}" length="50"/></a></td>
<td><c:out value="${row.consumerCount}" /></td>
<td><c:out value="${row.enqueueCount}" /></td>
<td><c:out value="${row.dequeueCount}" /></td>
<td>
    <a href="<c:url value="send.jsp">
                        <c:param name="JMSDestination" value="${row.name}" />
                        <c:param name="JMSDestinationType" value="topic"/></c:url>">Send To</a>
    <a href="<c:url value="topicSubscribers.jsp">
                        <c:param name="JMSDestination" value="${row.name}" /></c:url>">Active Subscribers</a><br/>
    <a href="<c:url value="topicProducers.jsp">
                        <c:param name="JMSDestination" value="${row.name}" /></c:url>">Active Producers</a><br/>
    <a href="<c:url value="deleteDestination.action">
                   <c:param name="JMSDestination" value="${row.name}" />
                   <c:param name="JMSDestinationType" value="topic"/>
                   <c:param name="secret" value='${sessionScope["secret"]}'/></c:url>">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>

<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
