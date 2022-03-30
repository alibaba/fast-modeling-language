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

import * as React from "react";
import {useState} from "react";
import {initX6, renderX6} from "./graphConfig";
import {Col, Divider, Layout, message, Row} from "antd";
import {ToolBar} from "../toolbar/ToolBar";
import {Editor} from "../editor/Editor";
import {languageID} from "../../fastmodel/config";
import {Content, Footer, Header} from "antd/es/layout/layout";
import {editor} from 'monaco-editor-core';
import {getServerRender} from "../../constants/server";
import {DataUri} from "@antv/x6";
import IModelContentChangedEvent = editor.IModelContentChangedEvent;

interface IFmlGraphProps {
}

const FmlGraph: React.FC<IFmlGraphProps> = (props: IFmlGraphProps) => {

    let divNode;

    const assignRef = React.useCallback((node) => {
        // On mount get the ref of the div and assign it the divNode
        divNode = node;
    }, []);

    const [gh, _setGh] = useState(null);

    const myStateRef = React.useRef(gh);

    const setGh = data => {
        myStateRef.current = data;
        _setGh(data);
    };

    React.useEffect(() => {
        if (divNode) {
            let x6 = initX6(divNode);
            setGh(x6);
        }
    }, [assignRef])

    const render = () => {
        let value = editor.getModels()[0].getValue();
        if (value) {
            getData(value);
        }
    }

    const getData = (value: string) => {
        getServerRender(value).then(data => {
            renderX6(myStateRef.current, data);
        }).catch(error => message.error(error.message));
    }

    const exportPng = () => {
        myStateRef.current.toPNG((dataUri) => {
            DataUri.downloadDataUri(dataUri, 'chart.png');
        }, {
            width: 800,
            height: 800,
            quality: 0.99,
            padding: {
                top: 20,
                right: 30,
                bottom: 40,
                left: 50,
            },
        });
    }


    let handle: any;

    const modelChangeContent = (event: IModelContentChangedEvent) => {
        clearTimeout(handle);
        handle = setTimeout(render, 500);
    }


    const importSqlCallback = (value: string) => {
        editor.getModels()[0].setValue(value);
    }

    const getEditorValue = (): string => {
        return editor.getModels()[0].getValue();
    }

    return <Layout className="layout">
        <Header>
            <div className="logo"/>
            <ToolBar render={render} importSql={importSqlCallback} getEditorValue={getEditorValue}
                     exportPng={exportPng}/>
        </Header>
        <Content style={{padding: '10px 0px'}}>
            <Row>
                <Col span={12}><Editor language={languageID} didChangeContent={modelChangeContent}
                                       initContent={render}/></Col>
                <Col span={12}>
                    <div ref={assignRef} style={{height: '90vh'}}></div>
                </Col>
            </Row>
        </Content>
        <Divider/>
        <Footer style={{textAlign: 'center'}}>Powered by ©2022 <a href={'https://kimball.alibaba-inc.com'}>建模引擎</a>
        </Footer>
    </Layout>
        ;
}

export
{
    FmlGraph
}
    ;