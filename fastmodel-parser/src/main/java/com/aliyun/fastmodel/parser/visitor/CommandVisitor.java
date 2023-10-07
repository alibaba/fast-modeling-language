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

package com.aliyun.fastmodel.parser.visitor;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.BaseCommandStatement;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.core.tree.statement.command.FormatFml;
import com.aliyun.fastmodel.core.tree.statement.command.HelpCommand;
import com.aliyun.fastmodel.core.tree.statement.command.ImportSql;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CommandOptionsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CommonSqlCommandStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DialectOptionsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.HelpCommandStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.Token;

/**
 * CommandVisitor
 *
 * @author panguanjing
 * @date 2021/12/1
 */
@SubVisitor
public class CommandVisitor extends AstBuilder {

    @Override
    public Node visitHelpCommandStatement(HelpCommandStatementContext ctx) {
        Optional<String> text = Optional.empty();
        if (ctx.TEXT_OPTION() != null) {
            StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
            text = Optional.of(stringLiteral.getValue());
        }
        Identifier command = (Identifier)visit(ctx.identifier());
        List<Property> list = ImmutableList.of();
        if (ctx.setProperties() != null) {
            list = ParserHelper.visit(this, ctx.setProperties().tableProperties().keyValueProperty(),
                Property.class);
        }
        return new HelpCommand(ParserHelper.getLocation(ctx), list, command, text);
    }

    @Override
    public Node visitCommonSqlCommandStatement(CommonSqlCommandStatementContext ctx) {
        DialectOptionsContext dialectOptionsContext = ctx.dialectOptions();
        Identifier identifier = null;
        if (dialectOptionsContext != null) {
            identifier = (Identifier)visit(dialectOptionsContext.identifier());
        } else {
            throw new ParseException("please setting the dialect name");
        }
        Optional<String> uri = Optional.empty();
        Optional<String> text = Optional.empty();
        CommandOptionsContext commandOptionsContext = ctx.commandOptions();
        if (commandOptionsContext.TEXT_OPTION() != null) {
            StringLiteral stringLiteral = (StringLiteral)visit(commandOptionsContext.string());
            text = Optional.of(stringLiteral.getValue());
        }
        if (commandOptionsContext.URI_OPTION() != null) {
            StringLiteral stringLiteral = (StringLiteral)visit(commandOptionsContext.string());
            uri = Optional.of(stringLiteral.getValue());
        }
        List<Property> list = getProperties(ctx.setProperties());

        Token type = ctx.commands().type;
        BaseCommandStatement baseCommandStatement;
        NodeLocation location = ParserHelper.getLocation(ctx);
        String origin = ParserHelper.getOrigin(ctx);
        switch (type.getType()) {
            case FastModelLexer.KW_IMPORT_SQL:
                baseCommandStatement = new ImportSql(location, origin,
                    identifier, uri, text, list);
                break;
            case FastModelLexer.KW_EXPORT_SQL:
                baseCommandStatement = new ExportSql(location, origin,
                    identifier, uri, text, list);
                break;
            case FastModelLexer.KW_FORMAT:
                baseCommandStatement = new FormatFml(location, list, text);
                break;
            default:
                throw new ParseException("Unexpected value: " + type.getType());
        }
        return baseCommandStatement;

    }

}
