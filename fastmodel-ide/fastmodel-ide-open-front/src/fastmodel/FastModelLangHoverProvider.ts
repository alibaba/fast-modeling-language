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
import {executeFml} from "../constants/server";
import {WorkerAccessor} from "./setup";

export default class FastModelLangHoverProvider
    implements monaco.languages.HoverProvider {
    constructor(private worker: WorkerAccessor) {
    }

    provideHover(
        model: monaco.editor.ITextModel,
        position: monaco.Position,
        token: monaco.CancellationToken
    ): monaco.languages.ProviderResult<monaco.languages.Hover> {
        const wordPosition = model.getWordAtPosition(position);
        return this.hover(wordPosition.word, model, position);
    }

    private async hover(
        word: string,
        model: monaco.editor.ITextModel,
        position: monaco.Position
    ): Promise<monaco.languages.Hover> {
        return executeFml("help keyword -t '" + word + "'")
            .then(data => {
                return {
                    range: new monaco.Range(
                        position.lineNumber,
                        position.column,
                        model.getLineCount(),
                        model.getLineMaxColumn(model.getLineCount())
                    ),
                    contents: [{value: "**Description**"}, {value: data}],
                };
            }).catch(error => {
                return {
                    range: new monaco.Range(
                        position.lineNumber,
                        position.column,
                        model.getLineCount(),
                        model.getLineMaxColumn(model.getLineCount())
                    ),
                    contents: [{value: "**Error**"}, {value: error.message}],
                };
            });
    }
}
