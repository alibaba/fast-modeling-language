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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.dict.CreateDict;
import com.aliyun.fastmodel.core.tree.statement.dict.DropDict;
import com.aliyun.fastmodel.core.tree.statement.dict.RenameDict;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictAlias;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictComment;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictProperties;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateDictContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropDataDictContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameDictContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDictAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDictCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDictPropertiesContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class DictVisitor extends AstBuilder {
    @Override
    public Node visitCreateDict(CreateDictContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        BaseDataType visit = (BaseDataType)visit(ctx.typeDbCol());
        Optional<BaseConstraint> baseConstraint = visitIfPresent(ctx.columnConstraintType(), BaseConstraint.class);
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement build = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .comment(comment)
            .aliasedName(aliasedName)
            .notExists(ctx.ifNotExists() != null)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreateDict(
            build,
            visit,
            baseConstraint.orElse(null),
            visitIfPresent(ctx.defaultValue(), BaseLiteral.class).orElse(null)
        );
    }

    @Override
    public Node visitRenameDict(RenameDictContext ctx) {
        return new RenameDict(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName())
        );
    }

    @Override
    public Node visitSetDictComment(SetDictCommentContext ctx) {
        return new SetDictComment(
            getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitSetDictProperties(SetDictPropertiesContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        return new SetDictProperties(
            getQualifiedName(ctx.qualifiedName()),
            properties,
            visitIfPresent(ctx.typeDbCol(), BaseDataType.class).orElse(null),
            visitIfPresent(ctx.columnConstraintType(), BaseConstraint.class).orElse(null),
            visitIfPresent(ctx.defaultValue(), BaseLiteral.class).orElse(null)
        );
    }

    @Override
    public Node visitDropDataDict(DropDataDictContext ctx) {
        return new DropDict(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSetDictAlias(SetDictAliasContext ctx) {
        return new SetDictAlias(
            getQualifiedName(ctx.qualifiedName()),
            (AliasedName)visit(ctx.setAliasedName())
        );
    }
}
