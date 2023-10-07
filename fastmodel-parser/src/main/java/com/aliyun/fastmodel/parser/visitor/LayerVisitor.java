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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.layer.AddChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.Checker;
import com.aliyun.fastmodel.core.tree.statement.layer.CheckerType;
import com.aliyun.fastmodel.core.tree.statement.layer.CreateLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.DropChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.DropLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.RenameLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerAlias;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerComment;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddLayerCheckerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CheckerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateLayerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropLayerCheckerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropLayerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameLayerContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetLayerAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetLayerCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetLayerPropertiesContext;
import com.google.common.collect.ImmutableList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class LayerVisitor extends AstBuilder {

    @Override
    public Node visitCreateLayer(CreateLayerContext ctx) {
        List<Property> list = getProperties(ctx.setProperties());
        List<Checker> visit = ImmutableList.of();
        if (ctx.checkers() != null) {
            visit = visit(ctx.checkers().checker(), Checker.class);
        }
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement element = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(list)
            .comment(comment)
            .aliasedName(aliasedName)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreateLayer(
            element,
            visit
        );
    }

    @Override
    public Node visitChecker(CheckerContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.type);
        Identifier name = (Identifier)visit(ctx.name);
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new Checker(
            CheckerType.getByCode(identifier.getValue()),
            stringLiteral,
            name,
            visitIfPresent(ctx.comment(), Comment.class).orElse(null),
            true
        );
    }

    @Override
    public Node visitSetLayerComment(SetLayerCommentContext ctx) {
        return new SetLayerComment(
            getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitRenameLayer(RenameLayerContext ctx) {
        return new RenameLayer(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName((ctx.alterStatementSuffixRename().qualifiedName()))
        );
    }

    @Override
    public Node visitSetLayerProperties(SetLayerPropertiesContext ctx) {
        return new SetLayerProperties(
            getQualifiedName(ctx.qualifiedName()),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitDropLayer(DropLayerContext ctx) {
        return new DropLayer(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitAddLayerChecker(AddLayerCheckerContext ctx) {
        return new AddChecker(
            getQualifiedName(ctx.qualifiedName()),
            (Checker)visit(ctx.checker())
        );
    }

    @Override
    public Node visitDropLayerChecker(DropLayerCheckerContext ctx) {
        return new DropChecker(
            getQualifiedName(ctx.qualifiedName()),
            (Identifier)visit(ctx.identifier())
        );
    }

    @Override
    public Node visitSetLayerAlias(SetLayerAliasContext ctx) {
        return new SetLayerAlias(getQualifiedName(ctx.qualifiedName()), (AliasedName)visit(ctx.setAliasedName()));
    }
}
