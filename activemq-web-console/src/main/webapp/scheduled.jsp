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
<%-- Workaround for https://ops4j1.jira.com/browse/PAXWEB-1070 --%>
<%@include file="WEB-INF/jspf/headertags.jspf" %>
<html>
<head>
<c:set var="pageTitle" value="Messages Scheduled for Future Delivery"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>


<c:choose>
<c:when test="${requestContext.brokerQuery.jobSchedulerStarted}">
<div style="margin-top: 5em">
	<table id="Jobs" class="sortable autostripe">
	<thead>
		<tr>
			<th>Job ID</th>
			<th>Cron Entry</th>
			<th>next scheduled time</th>
			<th>start</th>
			<th>delay</th>
			<th>period</th>
			<th>repeat</th>
			<th>Operations</th>
		</tr>
	</thead>
	<tbody>
		<c:forEach items="${requestContext.brokerQuery.scheduledJobs}"
		var="row">
		<tr>
		 <td><c:out value="${row.jobId}"/></td>
		 <td><c:out value="${row.cronEntry}"/></td>
		 <td><c:out value="${row.nextExecutionTime}"/></td>
		 <td><c:out value="${row.start}"/></td>
		 <td><c:out value="${row.delay}"/></td>
	 	 <td><c:out value="${row.period}"/></td>
	     <td><c:out value="${row.repeat}"/></td>
		<td>
		    <a href="<c:url value="deleteJob.action?jobId=${row.jobId}&secret=${sessionScope['secret']}"/>">Delete</a>
		</td>
	    </tr>
	</c:forEach>
	</tbody>
	</table>
</c:when>
<c:otherwise>
<div style="margin-top: 5em">
<p align="center">Scheduler not started!</p>
</div>
</c:otherwise>
</c:choose>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
