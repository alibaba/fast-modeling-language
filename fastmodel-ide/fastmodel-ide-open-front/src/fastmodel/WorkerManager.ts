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
import * as monaco from "monaco-editor-core";
import {Uri} from "monaco-editor-core";
import {FastmodelWorker} from "./fastmodelWorker";
import {languageID} from "./config";

export class WorkerManager {
    private worker: monaco.editor.MonacoWebWorker<FastmodelWorker>;
    private workerClientProxy: Promise<FastmodelWorker>;

    constructor() {
        this.worker = null;
    }

    private getClientproxy(): Promise<FastmodelWorker> {
        if (!this.workerClientProxy) {
            this.worker = monaco.editor.createWebWorker<FastmodelWorker>({
                // module that exports the create() method and returns a `JSONWorker` instance
                moduleId: 'FastmodelWorker',
                label: languageID,
                // passed in to the create() method
                createData: {
                    languageId: languageID,
                }
            });

            this.workerClientProxy = <Promise<FastmodelWorker>><any>this.worker.getProxy();
        }
        return this.workerClientProxy;
    }

    async getLanguageServiceWorker(...resources: Uri[]): Promise<FastmodelWorker> {
        const _client: FastmodelWorker = await this.getClientproxy();
        await this.worker.withSyncedResources(resources)
        return _client;
    }
}