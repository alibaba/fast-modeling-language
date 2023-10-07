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

import {ANTLRErrorListener, RecognitionException, Recognizer} from "antlr4ts";


export interface ILangError {
    startLineNumber: number;
    startColumn: number;
    endLineNumber: number;
    endColumn: number;
    message: string;
    code: string;
}

export default class LangErrorListener implements ANTLRErrorListener<any>{
    private errors: ILangError[] = []
    syntaxError(recognizer: Recognizer<any, any>, offendingSymbol: any, line: number, charPositionInLine: number, message: string, e: RecognitionException | undefined): void {

        this.errors.push(
            {
                startLineNumber:line,
                endLineNumber: line,
                startColumn: charPositionInLine,
                endColumn: charPositionInLine+1,//Let's suppose the length of the error is only 1 char for simplicity
                message,
                code: "1" // This the error code you can customize them as you want
            }
        )
    }

    getErrors(): ILangError[] {
        return this.errors;
    }
}