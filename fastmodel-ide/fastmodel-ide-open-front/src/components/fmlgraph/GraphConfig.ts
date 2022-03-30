/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import {Graph} from '@antv/x6';
import {DagreLayout} from "@antv/layout";
import {TableVertex} from "./GraphNode";

const LINE_HEIGHT = 24
const SP = "_";
export const GRAPH_WIDTH = 1000, GRAPH_HEIGHT = 800, NODE_WIDTH = 150, NODE_HEIGHT = 24;
const RANK_WIDTH = 60, NODE_SP_WIDTH = 80;


initNode();
let shapeVertexMap = new Map();
initMap();

/**
 * init map
 */
function initMap() {
    shapeVertexMap.set('DIM', 'dim_react');
    shapeVertexMap.set('FACT', 'fact_react');
    shapeVertexMap.set('DWS', 'dws_react');
    shapeVertexMap.set('ADS', 'ads_react');
}

/**
 *  init node config
 */
function initNode() {

    Graph.registerPortLayout(
        'erPortPosition',
        (portsPositionArgs) => {
            return portsPositionArgs.map((_, index) => {
                return {
                    position: {
                        x: 0,
                        y: (index + 1) * LINE_HEIGHT,
                    },
                    angle: 0,
                }
            })
        },
        true,
    )

    customNode("dim_react", {
        rect: {
            strokeWidth: 1,
            stroke: '#FFA40F',
            fill: '#FFA40F',
        },
        label: {
            fontWeight: 'bold',
            fill: '#ffffff',
            fontSize: 12,
        },
    }, '#FFA40F');

    customNode("fact_react", {
        rect: {
            strokeWidth: 1,
            stroke: '#00C1DE',
            fill: '#00C1DE',
        },
        label: {
            fontWeight: 'bold',
            fill: '#ffffff',
            fontSize: 12,
        },
    }, '#00C1DE');

    customNode("dws_react", {
        rect: {
            strokeWidth: 1,
            stroke: '#35B534',
            fill: '#35B534',
        },
        label: {
            fontWeight: 'bold',
            fill: '#ffffff',
            fontSize: 12,
        },
    }, '#35B534');

    customNode("ads_react", {
        rect: {
            strokeWidth: 1,
            stroke: '#8756FF',
            fill: '#8756FF',
        },
        label: {
            fontWeight: 'bold',
            fill: '#ffffff',
            fontSize: 12,
        },
    }, '#8756FF');

}

function customNode(nodeName, attrs, stroke = '#5F95FF') {
    Graph.registerNode(
        nodeName,
        {
            inherit: 'rect',
            markup: [
                {
                    tagName: 'rect',
                    selector: 'body',
                },
                {
                    tagName: 'text',
                    selector: 'label',
                },
            ],
            attrs: attrs,
            ports: {
                groups: {
                    list: {
                        markup: [
                            {
                                tagName: 'rect',
                                selector: 'portBody',
                            },
                            {
                                tagName: 'text',
                                selector: 'portNameLabel',
                            },
                            {
                                tagName: 'text',
                                selector: 'portPrimary'
                            },
                            {
                                tagName: 'text',
                                selector: 'portTypeLabel',
                            },
                        ],
                        attrs: {
                            portBody: {
                                width: NODE_WIDTH,
                                height: LINE_HEIGHT,
                                strokeWidth: 1,
                                stroke: stroke,
                                fill: '#EFF4FF',
                                magnet: true,
                            },
                            portNameLabel: {
                                ref: 'portBody',
                                refX: 6,
                                refY: 6,
                                fontSize: 10
                            },
                            portPrimary: {
                                ref: 'portBody',
                                refX: 1,
                                refY: 6,
                                fontSize: 10,
                                fill: '#ff0000'
                            },
                            portTypeLabel: {
                                ref: 'portBody',
                                refX: 95,
                                refY: 6,
                                fontSize: 10,
                            },
                        },
                        position: 'erPortPosition',
                    },
                },
            },
        },
        true,
    )
}

export function initX6(container) {
    const graph = new Graph({
        container: container,
        width: GRAPH_WIDTH,
        height: GRAPH_HEIGHT,
        grid: 10,
        mousewheel: {
            enabled: true,
            modifiers: ["ctrl", "meta"],
        },
        scroller: {
            enabled: true,
        },
        connecting: {
            router: {
                name: "er",
                args: {
                    offset: 25,
                    direction: "H",
                },
            },
            allowNode: false,
            allowEdge: false,
            allowBlank: false,
            allowLoop: false,
            allowPort: false
        },
    });
    return graph;
}

const dagreLayout = new DagreLayout({
    type: 'dagre',
    rankdir: 'LR',
    align: 'UL',
    ranksep: RANK_WIDTH,
    nodesep: NODE_SP_WIDTH,
    controlPoints: true,
})


export function renderX6(graph, data) {
    const wrapper = {
        nodes: [],
        edges: [],
    };
    showGraph(wrapper, data);
    const model = dagreLayout.layout(wrapper)
    graph.fromJSON(model);
}

const getDisplayName = (id: string, name: string) => {
    if (name) {
        return id + '(' + name + ')';
    } else {
        return id;
    }
}


function toVertex(n: TableVertex) {
    let tableName = n.attribute.name;
    let id = n.id;
    let vertexType = n.vertexType;
    let shape = shapeVertexMap.get(vertexType);
    let vertex = {
        id: id,
        width: NODE_WIDTH,
        height: NODE_HEIGHT,
        shape: shape,
        attrs: {
            label: {
                text: getDisplayName(id, tableName)
            },
        },
        ports: []
    }
    let ports = [];
    let column = n.attribute.columns;
    for (const c of column) {
        let port = attrs(n, c);
        ports.push(port);
    }
    vertex.ports = ports;
    return {vertex};
}

function showGraph(wrapper, data) {
    let node = data.nodes;
    for (const n of node) {
        let {vertex} = toVertex(n);
        wrapper.nodes.push(vertex);
    }
    let edges = data.edges || [];
    for (const e of edges) {
        let edge = toPortEdge(e);
        wrapper.edges.push(edge);
    }
}

function toPortEdge(e) {
    var src = e.source;
    var target = e.target;
    return {
        shape: "edge",
        source: {
            cell: src,
            port: src + SP + e.attribute.left
        },
        target: {
            cell: target,
            port: target + SP + e.attribute.right
        },
        attrs: {
            "line": {
                "stroke": "#A2B1C3",
                "strokeWidth": 1
            }
        }
        , zIndex: 0
    }
}


function attrs(n, column) {
    return {
        id: n.id + SP + column.colName,
        group: "list",
        attrs: {
            "portBody": {},
            "portNameLabel": {
                "text": column.colName
            },
            "portPrimary": {
                "text": column.primaryKey ? '*' : ''
            },
            "portTypeLabel": {
                "text": column.colType
            }
        }
    }
}
