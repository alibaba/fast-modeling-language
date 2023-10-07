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

package com.aliyun.fastmodel.transform.mysql.parser;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;

/**
 * oracle Parser
 *
 * @author panguanjing
 * @date 2021/7/24
 */
@AutoService(LanguageParser.class)
public class MysqlTransformerParser implements LanguageParser<Node, ReverseContext> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String text) throws ParseException {
        return parseNode(text, ReverseContext.builder().build());
    }

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        CodePointCharStream charStream = CharStreams.fromString(text);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        MySqlLexer lexer = new MySqlLexer(caseChangingCharStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        MySqlParser fastModelGrammarParser = new MySqlParser(commonTokenStream);
        fastModelGrammarParser.removeErrorListeners();
        fastModelGrammarParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = fastModelGrammarParser.root();
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = fastModelGrammarParser.root();
        }
        return tree.accept(new MysqlAstBuilder(context));
    }

}
