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
import {WorkerAccessor} from "./setup";
import {languageID} from "./config";
import {ILangError} from "../language-service/LangErrorListener";

export default class DiagnosticsAdapter {
    constructor(private worker: WorkerAccessor) {
        const onModelAdd = (model: monaco.editor.IModel): void => {
            let handle: any;
            model.onDidChangeContent(() => {
                // here we are Debouncing the user changes, so everytime a new change is done, we wait 500ms before validating
                // otherwise if the user is still typing, we cancel the
                clearTimeout(handle);
                handle = setTimeout(() => this.validate(model.uri), 500);
            });

            this.validate(model.uri);
        }
        monaco.editor.onDidCreateModel(onModelAdd);
        monaco.editor.getModels().forEach(onModelAdd);
    }

    private async validate(resource: monaco.Uri): Promise<void> {
        // get the worker proxy
        const worker = await this.worker(resource)
        // call the validate methode proxy from the langaueg service and get errors
        const errorMarkers = await worker.doValidation();
        // get the current model(editor or file) which is only one
        const model = monaco.editor.getModel(resource);
        // add the error markers and underline them with severity of Error
        monaco.editor.setModelMarkers(model, languageID, errorMarkers.map(toDiagnostics));

    }
}

function toDiagnostics(error: ILangError): monaco.editor.IMarkerData {
    return {
        ...error,
        severity: monaco.MarkerSeverity.Error,
    };
}