/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresAstBuilder;
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
public class HologresParser implements LanguageParser<Node, ReverseContext> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String code, ReverseContext context) throws ParseException {
        return getNode(code, context, PostgreSQLParser::root);
    }

    private Node getNode(String code, ReverseContext context, Function<PostgreSQLParser, ParserRuleContext> function) {
        CodePointCharStream charStream = CharStreams.fromString(code);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        PostgreSQLLexer lexer = new PostgreSQLLexer(caseChangingCharStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        PostgreSQLParser fastModelGrammarParser = new PostgreSQLParser(commonTokenStream);
        fastModelGrammarParser.removeErrorListeners();
        fastModelGrammarParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = function.apply(fastModelGrammarParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            fastModelGrammarParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = function.apply(fastModelGrammarParser);
        }
        return tree.accept(new HologresAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(code, context, PostgreSQLParser::typename);
    }

}
