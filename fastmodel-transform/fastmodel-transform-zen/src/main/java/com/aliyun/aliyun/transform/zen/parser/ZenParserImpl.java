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

package com.aliyun.aliyun.transform.zen.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenLexer;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.StringUtils;

/**
 * zen parser
 *
 * @author panguanjing
 * @date 2021/7/15
 */
@AutoService(LanguageParser.class)
public class ZenParserImpl implements ZenParser {
    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public <T> T parseNode(String text, Void context) throws ParseException {
        return (T)invokerParser(text, FastModelZenParser::zencoding);
    }

    @Override
    public <T> T parseNode(String zenCode) throws ParseException {
        return parseNode(zenCode, null);
    }

    private Node invokerParser(String dsl,
                               Function<FastModelZenParser, ParserRuleContext> parseFunction) {
        if (StringUtils.isBlank(dsl)) {
            throw new ParseException("dsl can't be blank");
        }
        String adjust = StripUtils.removeEmptyLine(dsl);
        CodePointCharStream input = CharStreams.fromString(adjust);
        CaseChangingCharStream charStream = new CaseChangingCharStream(input, true);
        FastModelZenLexer lexer = new FastModelZenLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        FastModelZenParser fastModelZenParser = new FastModelZenParser(commonTokenStream);
        fastModelZenParser.removeErrorListeners();
        fastModelZenParser.addErrorListener(LISTENER);

        ParserRuleContext tree;
        try {
            fastModelZenParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(fastModelZenParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            fastModelZenParser.reset();
            fastModelZenParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parseFunction.apply(fastModelZenParser);
        }
        return new ZenAstBuilder().visit(tree);
    }

}
