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
import com.aliyun.fastmodel.core.tree.statement.constants.MaterializedType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.materialize.CreateMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.DropMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.RenameMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeAlias;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeComment;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeRefProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateMaterializeStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropMaterializeStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameMaterializeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMaterializeAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMaterializeCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMaterializePropertiesContext;

/**
 * MaterializeVisitor
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class MaterializeVisitor extends AstBuilder {

    @Override
    public Node visitCreateMaterializeStatement(CreateMaterializeStatementContext ctx) {
        List<QualifiedName> list = getQualifiedName(ctx.tableNameList());
        List<Property> properties = null;
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement element = CreateElement.builder()
            .comment(comment)
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .aliasedName(aliasedName)
            .properties(properties)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreateMaterialize(
            element,
            list,
            ((Identifier)visit(ctx.identifier())),
            MaterializedType.TABLE
        );
    }

    @Override
    public Node visitRenameMaterialize(RenameMaterializeContext ctx) {
        return new RenameMaterialize(getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName())
        );
    }

    @Override
    public Node visitSetMaterializeComment(SetMaterializeCommentContext ctx) {
        return new SetMaterializeComment(getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitSetMaterializeProperties(SetMaterializePropertiesContext ctx) {
        List<Property> visit = visit(ctx.alterMaterializeProperties().tableProperties().keyValueProperty(),
            Property.class);
        List<QualifiedName> qualifiedNames = null;
        if (ctx.tableNameList() != null) {
            qualifiedNames = getQualifiedName(ctx.tableNameList());
        }
        Identifier identifier = visitIfPresent(ctx.identifier(), Identifier.class).orElse(null);
        return new SetMaterializeRefProperties(getQualifiedName(ctx.qualifiedName()),
            visit,
            qualifiedNames,
            MaterializedType.TABLE,
            identifier
        );
    }

    @Override
    public Node visitDropMaterializeStatement(DropMaterializeStatementContext ctx) {
        MaterializedType type = MaterializedType.TABLE;
        return new DropMaterialize(getQualifiedName(ctx.qualifiedName()), type);
    }

    @Override
    public Node visitSetMaterializeAlias(SetMaterializeAliasContext ctx) {
        return new SetMaterializeAlias(
            getQualifiedName(ctx.qualifiedName()),
            (AliasedName)visit(ctx.setAliasedName())
        );
    }
}
