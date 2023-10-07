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
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.adjunct.CreateAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.DropAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.RenameAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctAlias;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctComment;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctProperties;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateAdjunctContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropAdjunctContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameAdjunctContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetAdjunctAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetAdjunctCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetAdjunctPropertiesContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class AdjunctVisitor extends AstBuilder {

    @Override
    public Node visitCreateAdjunct(CreateAdjunctContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        BaseExpression baseExpression = visitIfPresent(ctx.expression(), BaseExpression.class).orElse(null);
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement element = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .comment(comment)
            .aliasedName(aliasedName)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreateAdjunct(
            element, baseExpression
        );
    }

    @Override
    public Node visitRenameAdjunct(RenameAdjunctContext ctx) {
        return new RenameAdjunct(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName((ctx.alterStatementSuffixRename().qualifiedName()))
        );
    }

    @Override
    public Node visitSetAdjunctComment(SetAdjunctCommentContext ctx) {
        return new SetAdjunctComment(
            getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitSetAdjunctProperties(SetAdjunctPropertiesContext ctx) {
        List<Property> list = getProperties(ctx.setProperties());
        return new SetAdjunctProperties(getQualifiedName(ctx.qualifiedName()),
            list, visitIfPresent(ctx.expression(), BaseExpression.class).orElse(null)
        );
    }

    @Override
    public Node visitDropAdjunct(DropAdjunctContext ctx) {
        return new DropAdjunct(
            getQualifiedName(ctx.qualifiedName())
        );
    }

    @Override
    public Node visitSetAdjunctAlias(SetAdjunctAliasContext ctx) {
        return new SetAdjunctAlias(getQualifiedName(ctx.qualifiedName()),
            (AliasedName)visit(ctx.setAliasedName()));
    }

}
