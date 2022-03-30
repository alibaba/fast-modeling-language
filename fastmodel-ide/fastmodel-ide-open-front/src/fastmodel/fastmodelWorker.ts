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

import {worker} from "monaco-editor-core";
import LanguageService from "../language-service/LanguageService";
import {ILangError} from "../language-service/LangErrorListener";
import IWorkerContext = worker.IWorkerContext;

export class FastmodelWorker {
    private _ctx: IWorkerContext;
    private languageService: LanguageService;

    constructor(ctx: IWorkerContext) {
        this._ctx = ctx;
        this.languageService = new LanguageService();
    }

    doValidation(): Promise<ILangError[]> {
        const code = this.getTextDocument();
        return Promise.resolve(this.languageService.validate(code));
    }

    format(code: string): Promise<string> {
        return Promise.resolve(this.languageService.format(code));
    }

    private getTextDocument(): string {
        const model = this._ctx.getMirrorModels()[0];// When there are multiple files open, this will be an array
        return model.getValue();
    }
}