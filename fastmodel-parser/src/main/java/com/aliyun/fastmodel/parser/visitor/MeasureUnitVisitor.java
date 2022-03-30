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
import com.aliyun.fastmodel.core.tree.statement.measure.unit.CreateMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.DropMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.RenameMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitAlias;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitComment;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateMeasureUnitContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropMeasureUnitContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameMeasureUnitContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMeasureUnitAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMeasureUnitCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetMeasureUnitPropertiesContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class MeasureUnitVisitor extends AstBuilder {
    @Override
    public Node visitCreateMeasureUnit(CreateMeasureUnitContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        return new CreateMeasureUnit(
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.qualifiedName()))
                .properties(properties)
                .comment(comment)
                .notExists(ctx.ifNotExists() != null)
                .createOrReplace(ctx.replace() != null)
                .build()
        );
    }

    @Override
    public Node visitRenameMeasureUnit(RenameMeasureUnitContext ctx) {
        return new RenameMeasureUnit(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName())
        );
    }

    @Override
    public Node visitSetMeasureUnitComment(SetMeasureUnitCommentContext ctx) {
        return new SetMeasureUnitComment(getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitSetMeasureUnitProperties(SetMeasureUnitPropertiesContext ctx) {
        return new SetMeasureUnitProperties(
            getQualifiedName(ctx.qualifiedName()),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitDropMeasureUnit(DropMeasureUnitContext ctx) {
        return new DropMeasureUnit(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSetMeasureUnitAlias(SetMeasureUnitAliasContext ctx) {
        return new SetMeasureUnitAlias(getQualifiedName(ctx.qualifiedName()), (AliasedName)visit(ctx.setAliasedName()));
    }
}
