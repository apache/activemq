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
function drawGraph() {
    var layout = new PlotKit.Layout("bar", {});
    
    layout.addDataset(
    	"Queue Sizes", 
        [ <c:forEach items="${requestContext.brokerQuery.queues}" var="row" varStatus="status"> [${status.count}, 
${row.queueSize}], </c:forEach> ] );
    layout.evaluate();
    
var options = {
   "IECanvasHTC": "<c:url value="/js/plotkit/iecanvas.htc"/>",
   "colorScheme": PlotKit.Base.palette(PlotKit.Base.baseColors()[0]),
   "padding": {left: 0, right: 0, top: 10, bottom: 30},
   "xTicks": [
         <c:forEach items="${requestContext.brokerQuery.queues}" var="row" varStatus="status">     {v:${status.count}, label:"${row.name}"},  </c:forEach>
          ],
   "drawYAxis": false
};

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
	