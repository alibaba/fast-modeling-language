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
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.group.CreateGroup;
import com.aliyun.fastmodel.core.tree.statement.group.DropGroup;
import com.aliyun.fastmodel.core.tree.statement.group.GroupType;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupAlias;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupComment;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateGroupContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropGroupContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetGroupAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetGroupCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetGroupPropertiesContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class GroupVisitor extends AstBuilder {

    @Override
    public Node visitCreateGroup(CreateGroupContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement build = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .aliasedName(aliasedName)
            .comment(comment)
            .notExists(ctx.ifNotExists() != null)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreateGroup(
            build,
            GroupType.getByCode(ctx.type.getText()
            )
        );
    }

    @Override
    public Node visitSetGroupComment(SetGroupCommentContext ctx) {
        return new SetGroupComment(
            getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment()),
            GroupType.getByCode(ctx.type.getText())
        );
    }

    @Override
    public Node visitSetGroupProperties(SetGroupPropertiesContext ctx) {
        return new SetGroupProperties(
            getQualifiedName(ctx.qualifiedName()),
            getProperties(ctx.setProperties()),
            GroupType.getByCode(ctx.type.getText())
        );
    }

    @Override
    public Node visitDropGroup(DropGroupContext ctx) {
        return new DropGroup(
            getQualifiedName(ctx.qualifiedName()),
            GroupType.getByCode(ctx.type.getText())
        );
    }

    @Override
    public Node visitSetGroupAlias(SetGroupAliasContext ctx) {
        return new SetGroupAlias(getQualifiedName(ctx.qualifiedName()),
            (AliasedName)visit(ctx.setAliasedName()));
    }
}
