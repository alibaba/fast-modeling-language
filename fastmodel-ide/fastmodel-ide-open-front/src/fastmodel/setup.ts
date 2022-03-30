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
import {languageExtensionPoint, languageID} from "./config";
import {monarchLanguage, richLanguageConfiguration} from "./FastModelLang";
import {WorkerManager} from "./WorkerManager";
import {FastmodelWorker} from "./fastmodelWorker";
import DiagnosticsAdapter from "./DiagnosticsAdapter";
import FastModelLangFormattingProvider from "./FastModelLangFormattingProvider";
import FastModelLangHoverProvider from "./FastModelLangHoverProvider";

export function setupLanguage() {
    (window as any).MonacoEnvironment = {
        getWorkerUrl: function (moduleId, label) {
            if (label === languageID) {
                return "./fastmodelWorker.js";
            }
            return './editor.worker.js';
        }
    }
    monaco.languages.register(languageExtensionPoint);
    monaco.languages.onLanguage(languageID, () => {
        monaco.languages.setMonarchTokensProvider(languageID, monarchLanguage);
        monaco.languages.setLanguageConfiguration(languageID, richLanguageConfiguration);
        const client = new WorkerManager();
        const worker: WorkerAccessor = (...uris: monaco.Uri[]): Promise<FastmodelWorker> => {
            return client.getLanguageServiceWorker(...uris);
        };
        new DiagnosticsAdapter(worker);
        monaco.languages.registerDocumentFormattingEditProvider(languageID, new FastModelLangFormattingProvider(worker));
        monaco.languages.registerHoverProvider(languageID, new FastModelLangHoverProvider(worker));
    });
}

export type WorkerAccessor = (...uris: monaco.Uri[]) => Promise<FastmodelWorker>;