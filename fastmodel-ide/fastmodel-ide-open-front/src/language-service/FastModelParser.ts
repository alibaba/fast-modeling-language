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

import {FastModelGrammarParser, RootContext} from "../antlr/FastModelGrammarParser";
import {FastModelLexer} from "../antlr/FastModelLexer";
import {ANTLRInputStream, CommonTokenStream} from "antlr4ts";
import LangErrorListener, {ILangError} from "./LangErrorListener";

function parse(code: string): { ast: RootContext, errors: ILangError[] } {
    const inputStream = new ANTLRInputStream(code);
    const lexer = new FastModelLexer(inputStream);
    lexer.removeErrorListeners()
    const langErrorListener = new LangErrorListener();
    lexer.addErrorListener(langErrorListener);
    const tokenStream = new CommonTokenStream(lexer);
    const parser = new FastModelGrammarParser(tokenStream);
    parser.removeErrorListeners();
    parser.addErrorListener(langErrorListener);
    const ast = parser.root();
    const errors: ILangError[] = langErrorListener.getErrors();
    return {ast, errors};
}

export function parseAndGetASTRoot(code: string): RootContext {
    const {ast} = parse(code);
    return ast;
}

export function parseAndGetSyntaxErrors(code: string): ILangError[] {
    const {errors} = parse(code);
    return errors;
}