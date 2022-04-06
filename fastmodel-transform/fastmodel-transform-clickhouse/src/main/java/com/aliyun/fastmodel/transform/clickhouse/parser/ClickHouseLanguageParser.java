/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.visitor.ClickHouseAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;

/**
 * ClickHouseLanguageParser
 *
 * @author panguanjing
 * @date 2022/7/9
 */
@AutoService(LanguageParser.class)
public class ClickHouseLanguageParser implements LanguageParser<Node, ReverseContext> {

    private static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, ClickHouseParser::queryStmt);
    }

    private Node getNode(String code, ReverseContext context, Function<ClickHouseParser, ParserRuleContext> function) {
        CodePointCharStream charStream = CharStreams.fromString(code);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        ClickHouseLexer clickHouseLexer = new ClickHouseLexer(caseChangingCharStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(clickHouseLexer);
        ClickHouseParser clickHouseParser = new ClickHouseParser(commonTokenStream);
        clickHouseParser.removeErrorListeners();
        clickHouseParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            clickHouseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = function.apply(clickHouseParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            clickHouseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = function.apply(clickHouseParser);
        }
        return tree.accept(new ClickHouseAstBuilder(context));
    }
}
