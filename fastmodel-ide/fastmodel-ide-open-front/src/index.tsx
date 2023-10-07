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
import * as ReactDOM from 'react-dom';
import {setupLanguage} from "./fastmodel/setup";
import 'antd/dist/antd.css';
import {FmlGraph} from "./components/fmlgraph/FmlGraph";
import {message} from 'antd';

message.config({
    top: 100,
    duration: 2,
    maxCount: 3,
    rtl: true
});

setupLanguage();

ReactDOM.render(
    <FmlGraph/>
    , document.getElementById('container'));