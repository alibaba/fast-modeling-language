/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/13
 */
@AutoService(LanguageParser.class)
public class AdbPostgreSQLLanguageParser implements LanguageParser<Node, ReverseContext> {
    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, AdbPostgreSQLParser::root);
    }

    private Node getNode(String text, ReverseContext context, Function<AdbPostgreSQLParser, ParserRuleContext> functionalInterface) {
        ParserRuleContext tree = ParserHelper.getNode(
            text,
            AdbPostgreSQLLexer::new,
            AdbPostgreSQLParser::new,
            parser -> {
                AdbPostgreSQLParser starRocksParser = (AdbPostgreSQLParser)parser;
                return functionalInterface.apply(starRocksParser);
            }
        );
        return tree.accept(new AdbPostgreSQLAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(code, context, AdbPostgreSQLParser::typename);
    }

    @Override
    public <T> T parseExpression(String code) throws ParseException {
        return (T)getNode(code, ReverseContext.builder().build(), AdbPostgreSQLParser::a_expr);
    }
}
