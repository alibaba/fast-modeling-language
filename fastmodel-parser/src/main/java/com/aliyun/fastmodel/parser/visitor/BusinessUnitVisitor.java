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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.businessunit.CreateBusinessUnit;
import com.aliyun.fastmodel.core.tree.statement.businessunit.SetBusinessUnitAlias;
import com.aliyun.fastmodel.core.tree.statement.businessunit.SetBusinessUnitComment;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBuStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBuAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBuCommentContext;
import com.google.common.collect.ImmutableList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class BusinessUnitVisitor extends AstBuilder {
    @Override
    public Node visitCreateBuStatement(CreateBuStatementContext ctx) {
        QualifiedName qu = getQualifiedName(ctx.qualifiedName());
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        List<Property> list = getProperties(ctx.setProperties());
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        return new CreateBusinessUnit(CreateElement.builder()
            .notExists(ctx.ifNotExists() != null)
            .properties(list)
            .comment(comment)
            .qualifiedName(qu)
            .aliasedName(aliasedName)
            .createOrReplace(ctx.replace() != null)
            .build());
    }

    @Override
    public Node visitSetBuComment(SetBuCommentContext ctx) {
        Comment comment = (Comment)visit(ctx.alterStatementSuffixSetComment());
        Identifier visit = (Identifier)visit(ctx.identifier());
        return new SetBusinessUnitComment(QualifiedName.of(ImmutableList.of(visit)),
            comment);
    }

    @Override
    public Node visitSetBuAlias(SetBuAliasContext ctx) {
        Identifier visit = (Identifier)visit(ctx.identifier());
        return new SetBusinessUnitAlias(
            QualifiedName.of(ImmutableList.of(visit)),
            (AliasedName)visit(ctx.setAliasedName())
        );
    }
}
