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

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * mysql Parser
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
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new MySqlLexer(charStream),
            tokenStream -> new MySqlParser(tokenStream),
            parser -> {
                MySqlParser mySqlParser = (MySqlParser)parser;
                return mySqlParser.root();
            }
        );
        return parserRuleContext.accept(new MysqlAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, charStream -> new MySqlLexer(charStream),
            tokenStream -> new MySqlParser(tokenStream),
            parser -> {
                MySqlParser mySqlParser = (MySqlParser)parser;
                return mySqlParser.dataType();
            }
        );
        return (BaseDataType)parserRuleContext.accept(new MysqlAstBuilder(context));
    }

}