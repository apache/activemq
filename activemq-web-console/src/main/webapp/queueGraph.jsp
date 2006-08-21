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

    <c:set var="disableJavaScript" value="true" scope="request"/>

   <script src='<c:url value="/js/mochi/MochiKit.js"/>' type="text/javascript"></script>
   <script src='<c:url value="/js/plotkit/Base.js"/>' type="text/javascript"></script>
   <script src='<c:url value="/js/plotkit/Layout.js"/>' type="text/javascript"></script>
   <script src='<c:url value="/js/plotkit/Canvas.js"/>' type="text/javascript"></script>
   <script src='<c:url value="/js/plotkit/SweetCanvas.js"/>' type="text/javascript"></script>
</head>
<body>

<script>
var options = {
   "IECanvasHTC": "<c:url value="/js/plotkit/iecanvas.htc"/>",
   "colorScheme": PlotKit.Base.palette(PlotKit.Base.baseColors()[0]),
   "padding": {left: 0, right: 0, top: 10, bottom: 30},
   "xTicks": [<c:forEach items="${requestContext.brokerQuery.queues}" var="row" varStatus="status"
         ><c:if 
         test="${status.count > 1}">, </c:if>{v:${status.count}, label:"${row.name}"}</c:forEach>]
};

function drawGraph() {
    var layout = new PlotKit.Layout("bar", options);
    
    layout.addDataset("sqrt",  [<c:forEach items="${requestContext.brokerQuery.queues}" var="row" varStatus="status"><c:if 
         test="${status.count > 1}">, </c:if> [${status.count},  ${row.queueSize}] </c:forEach> ]);
    layout.evaluate();
    
    var canvas = MochiKit.DOM.getElement("graph");
    var plotter = new PlotKit.SweetCanvasRenderer(canvas, layout, options);
    plotter.render();
}
MochiKit.DOM.addLoadEvent(drawGraph);
</script>

 <div><canvas id="graph" height="400" width="760"></canvas></div>
 
<%---    
Other values we can graph...

<td>${row.consumerCount}</td>
<td>${row.enqueueCount}</td>
<td>${row.dequeueCount}</td>
--%>


</body>
</html>
	
