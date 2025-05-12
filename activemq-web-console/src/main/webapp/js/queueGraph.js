/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function drawGraph() {
    const queues = JSON.parse(document.getElementById('queues').value);
    const options = {
        "IECanvasHTC": document.getElementById('IecCanvasHtcUrl').value,
        "colorScheme": PlotKit.Base.palette(PlotKit.Base.baseColors()[0]),
        "padding": {left: 0, right: 0, top: 10, bottom: 30},
        "xTicks": queues
    };

    const layout = new PlotKit.Layout("bar", options);

    const data = JSON.parse(document.getElementById('data').value)
    layout.addDataset("sqrt", data);
    layout.evaluate();

    const canvas = MochiKit.DOM.getElement("graph");
    const plotter = new PlotKit.SweetCanvasRenderer(canvas, layout, options);
    plotter.render();
}

MochiKit.DOM.addLoadEvent(drawGraph);