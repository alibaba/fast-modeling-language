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

export default class FastModelLangFormattingProvider
    implements monaco.languages.DocumentFormattingEditProvider {
    constructor(private worker: WorkerAccessor) {
    }

    provideDocumentFormattingEdits(
        model: monaco.editor.ITextModel,
        options: monaco.languages.FormattingOptions,
        token: monaco.CancellationToken
    ): monaco.languages.ProviderResult<monaco.languages.TextEdit[]> {
        return this.format(model.uri, model.getValue());
    }

    private async format(
        resource: monaco.Uri,
        code: string
    ): Promise<monaco.languages.TextEdit[]> {
        // get the worker proxy
        const worker = await this.worker(resource);
        //call the format method proxy from the language service
        const formattedCode = await worker.format(code);
        const endLineNumber = code.split("\n").length + 1;
        const endColumn = code.split("\n").map(line => line.length).sort((a, b) => a - b)[0] + 1;
        return [
            {
                text: formattedCode,
                range: {
                    endColumn,
                    endLineNumber,
                    startColumn: 0,
                    startLineNumber: 0
                }
            }
        ]
    }
}
