/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.IgnoreErrorListener;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext.ReverseTargetStrategy;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.mc.parser.visitor.MaxComputeAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;

import static com.aliyun.fastmodel.transform.mc.MaxComputeTransformer.LISTENER;

/**
 * MaxComputeLanguage Parser
 *
 * @author panguanjing
 * @date 2021/9/7
 */
@AutoService(LanguageParser.class)
@Dialect(DialectName.Constants.MAXCOMPUTE)
public class MaxComputeLanguageParser implements LanguageParser<Node, ReverseContext> {
    public static final IgnoreErrorListener IGNORE = new IgnoreErrorListener();

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        Function<OdpsParser, ParserRuleContext> parseFunction = context != null
            && context.getReverseRelationStrategy() == ReverseTargetStrategy.SCRIPT
            ? OdpsParser::script : OdpsParser::statement;
        return getNode(context, text, parseFunction, true);
    }

    private Node getNode(ReverseContext context, String code, Function<OdpsParser, ParserRuleContext> parseFunction,
                         boolean appendTerminator) {
        CommonTokenStream commonTokenStream = tokenize(code);
        if (appendTerminator) {
            int lastToken = commonTokenStream.size() - 2;
            while (lastToken >= 0 && commonTokenStream.get(lastToken).getChannel() != Token.DEFAULT_CHANNEL) {
                lastToken--;
            }
            if (lastToken < 0 || commonTokenStream.get(lastToken).getType() != OdpsLexer.SEMICOLON) {
                // A new line keeps the terminator outside a trailing line comment.
                commonTokenStream = tokenize(code + "\n;");
            }
        }
        OdpsParser fastModelGrammarParser = new OdpsParser(commonTokenStream);
        fastModelGrammarParser.removeErrorListeners();
        fastModelGrammarParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(fastModelGrammarParser);
        } catch (Throwable e) {
            fastModelGrammarParser.reset();
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parseFunction.apply(fastModelGrammarParser);
        }
        Token remaining = fastModelGrammarParser.getCurrentToken();
        if (remaining.getType() != Token.EOF) {
            throw new ParseException("Unexpected trailing token: " + remaining.getText(),
                remaining.getLine(), remaining.getCharPositionInLine());
        }
        return new MaxComputeAstBuilder(context).visit(tree);
    }

    private CommonTokenStream tokenize(String code) {
        OdpsLexer lexer = new OdpsLexer(CharStreams.fromString(code));
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        return tokens;
    }

    @Override
    public BaseExpression parseExpression(String text) throws ParseException {
        return (BaseExpression)getNode(ReverseContext.builder().build(), text, OdpsParser::expression, false);
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(context, code, OdpsParser::type, false);
    }
}
