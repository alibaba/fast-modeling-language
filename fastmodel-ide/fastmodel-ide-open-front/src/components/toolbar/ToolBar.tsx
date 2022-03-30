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

import * as React from 'react';
import {useState} from 'react';
import {Button, Col, Divider, Menu, message, Modal, Row, Select, Upload} from "antd";
import TextArea from "antd/lib/input/TextArea";
import {exportSql, importSql, Server} from "../../constants/server";
import {IInvokeResult} from "../../constants/models";
import {UploadOutlined} from '@ant-design/icons';


const {Option} = Select;


interface IToolbarProps {
    render: () => void
    getEditorValue: () => string
    exportPng: () => void
    importSql: (value: string) => void
}


const ToolBar: React.FC<IToolbarProps> = (props: IToolbarProps) => {
    const [isModalVisible, setIsModalVisible] = useState(false);

    const [isExportModalVisible, setIsExportModalVisible] = useState(false);

    const [inputValue, setInputValue] = useState("");

    const [selectValue, setSelectValue] = useState("");

    const [exportSqlValue, setExportSqlValue] = useState("");

    const [copied, setCopied] = useState(false);

    const showModal = () => {
        setIsModalVisible(true);
    };

    const showExportModal = () => {
        setIsExportModalVisible(true);

    }

    const handleOk = () => {
        if (!selectValue) {
            return message.error('Please select Dialect');
        }
        importSql(selectValue, inputValue)
            .then(result => {
                props.importSql(result);
                setIsModalVisible(false);
            }).catch(error => message.error(error.message));
    };

    const handleCancel = () => {
        setIsModalVisible(false);
    };

    const handleImportSelect = (value) => {
        setSelectValue(value);
    }

    const handleExportCancle = () => {
        setIsExportModalVisible(false);
    }


    const handleSelectExportChange = (value: string) => {
        let editorValue = props.getEditorValue();
        exportSql(value, editorValue).then(data => {
            setExportSqlValue(data);
            setCopied(false);
        }).catch(error => message.error(error.message));
    }


    const copyText = () => {
        navigator.clipboard.writeText(exportSqlValue);
        setCopied(true);
    };

    const uploadProps = {
        name: "file",
        action: Server.UPLOAD_FILE,
        headers: {
            authorization: "authorization-text",
        },
        showUploadList: false,
        data: {dialectName: selectValue},
        beforeUpload: (file) => {
            const isNotTextPlain = file.type && file.type !== "text/plain";
            if (isNotTextPlain) {
                message.error(`${file.name} is not a sql file`);
            }
            return !isNotTextPlain ? true : Upload.LIST_IGNORE;
        },
        onChange(info) {
            if (info.file.status !== "uploading") {
                console.log(info.file, info.fileList);
            }
            if (info.file.status === "done") {
                let result: IInvokeResult = info.file.response;
                message.success(`${info.file.name} file uploaded successfully`);
                setInputValue(result.data);
            } else if (info.file.status === "error") {
                let result: IInvokeResult = info.file.response;
                message.error(`${info.file.name} file upload failed.message:${result.message}`);
            }
        },
    };

    const showHelp = () => {
        let newWindow = window.open("https://fml.alibaba-inc.com/docs/#/zh-cn/", '_blank', 'noopener,noreferrer');
        if (newWindow) {
            newWindow.opener = null;
        }
    }

    return <>
        <Menu theme="dark" mode="horizontal">
            <Menu.Item onClick={props.render} key={`Refresh`}>Refresh</Menu.Item>
            <Menu.Item onClick={showModal} key={`Import`}>Import</Menu.Item>
            <Menu.Item onClick={showExportModal} key={`Export`}>Export</Menu.Item>
            <Menu.Item onClick={showHelp} key={`Help`}>Help</Menu.Item>
        </Menu>
        <Modal title="Import" visible={isModalVisible} onOk={handleOk} onCancel={handleCancel} width={800}>
            <Row>
                <Col span={18}>
                    <Select onChange={handleImportSelect} style={{width: 240}} placeholder={"select dialect"}>
                        <Option value="MaxCompute">MaxCompute</Option>
                        <Option value="Hive">Hive</Option>
                        <Option value="Mysql">Mysql</Option>
                        <Option value="Hologres">Hologres</Option>
                    </Select>
                </Col>
                <Col span={6}>
                    <Upload {...uploadProps}>
                        <Button icon={<UploadOutlined/>} type={"primary"}>Upload .sql</Button>
                    </Upload>
                </Col>
            </Row>
            <Divider orientation="left">SQL</Divider>
            <Row>
                <Col span={24}><TextArea rows={20} onChange={(e) => setInputValue(e.target.value)}
                                         value={inputValue}></TextArea></Col>
            </Row>
        </Modal>
        <Modal title="Export" visible={isExportModalVisible} onCancel={handleExportCancle}
               width={800}
               footer={[
                   <Button key="cancel" onClick={handleExportCancle}>Cancel</Button>,
                   <Button key="copy" onClick={copyText}>Copy</Button>
               ]}
        >
            <Row>
                <Col span={6}>
                    <Select onChange={handleSelectExportChange} style={{width: 180}} placeholder={"select dialect"}>
                        <Option value="MaxCompute">MaxCompute</Option>
                        <Option value="Hive">Hive</Option>
                        <Option value="Mysql">Mysql</Option>
                        <Option value="Hologres">Hologres</Option>
                    </Select>
                </Col>
                <Col span={6}>
                    <Button key="exportPng" type={"primary"} onClick={() => {
                        props.exportPng();
                        setIsExportModalVisible(false);
                    }}>Export PNG</Button>
                </Col>
            </Row>
            <Divider orientation="left">SQL</Divider>
            <Row>
                <Col span={24}><TextArea rows={20} readOnly={true} value={exportSqlValue}> </TextArea></Col>
            </Row>
            <Row>
                <Col>{copied ? <span style={{color: 'red'}}>Copied.</span> : null}</Col>
            </Row>
        </Modal>
    </>

}

export {ToolBar};