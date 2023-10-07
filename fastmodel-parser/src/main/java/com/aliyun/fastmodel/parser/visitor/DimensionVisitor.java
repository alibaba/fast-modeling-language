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

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.dimension.AddDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.ChangeDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.CreateDimension;
import com.aliyun.fastmodel.core.tree.statement.dimension.DropDimension;
import com.aliyun.fastmodel.core.tree.statement.dimension.DropDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.RenameDimension;
import com.aliyun.fastmodel.core.tree.statement.dimension.SetDimensionAlias;
import com.aliyun.fastmodel.core.tree.statement.dimension.SetDimensionComment;
import com.aliyun.fastmodel.core.tree.statement.dimension.SetDimensionProperties;
import com.aliyun.fastmodel.core.tree.statement.dimension.UnSetDimensionProperties;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.AttributeCategory;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddDimensionAttributeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AttributeCategoryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeDimensionAttributeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateDimensionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DimensionAttributeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropDimensionAttributeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropDimensionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameDimensionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDimensionAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDimensionCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDimensionPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetDimensionPropertiesContext;
import com.google.common.collect.ImmutableList;

/**
 * 维度定义相关的visitor
 *
 * @author panguanjing
 */
@SubVisitor
public class DimensionVisitor extends AstBuilder {
    @Override
    public Node visitCreateDimension(CreateDimensionContext ctx) {
        CreateElement createElement =
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.tableName()))
                .aliasedName(getAlias(ctx.alias()))
                .comment(getComment(ctx.comment()))
                .notExists(ctx.ifNotExists() != null)
                .properties(getProperties(ctx.setProperties()))
                .build();
        List<DimensionAttribute> list = ImmutableList.of();
        if (ctx.dimensionAttributeList() != null) {
            list = visit(ctx.dimensionAttributeList().dimensionAttribute(), DimensionAttribute.class);
        }
        return CreateDimension.builder()
            .createElement(createElement)
            .dimensionFields(list)
            .build();

    }

    @Override
    public Node visitDimensionAttribute(DimensionAttributeContext ctx) {
        AttributeCategory fieldCategory = AttributeCategory.BUSINESS;
        AttributeCategoryContext fieldCategoryContext = ctx.attributeCategory();
        if (fieldCategoryContext != null) {
            fieldCategory = AttributeCategory.valueOf(ctx.attributeCategory().getText().toUpperCase());
        }
        Boolean primary = null;
        if (ctx.attributeConstraint() != null) {
            primary = ctx.attributeConstraint().KW_PRIMARY() != null;
        }
        List<Property> properties = getProperties(ctx.setProperties());
        return DimensionAttribute.builder()
            .comment(getComment(ctx.comment()))
            .aliasName(getAlias(ctx.alias()))
            .attrName(getIdentifier(ctx.identifier()))
            .category(fieldCategory)
            .primary(primary)
            .properties(properties)
            .build();
    }

    @Override
    public Node visitAddDimensionAttribute(AddDimensionAttributeContext ctx) {
        return new AddDimensionAttribute(
            getQualifiedName(ctx.tableName()),
            visit(ctx.dimensionAttributeList().dimensionAttribute(), DimensionAttribute.class)
        );
    }

    @Override
    public Node visitRenameDimension(RenameDimensionContext ctx) {
        return new RenameDimension(
            getQualifiedName(ctx.tableName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName())
        );
    }

    @Override
    public Node visitSetDimensionProperties(SetDimensionPropertiesContext ctx) {
        return new SetDimensionProperties(
            getQualifiedName(ctx.tableName()),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitUnSetDimensionProperties(UnSetDimensionPropertiesContext ctx) {
        return new UnSetDimensionProperties(
            getQualifiedName(ctx.tableName()),
            getUnSetProopertyKeys(ctx.unSetProperties())
        );
    }

    @Override
    public Node visitSetDimensionComment(SetDimensionCommentContext ctx) {
        return new SetDimensionComment(
            getQualifiedName(ctx.tableName()),
            getComment(ctx.alterStatementSuffixSetComment().comment())
        );
    }

    @Override
    public Node visitDropDimensionAttribute(DropDimensionAttributeContext ctx) {
        return new DropDimensionAttribute(
            getQualifiedName(ctx.tableName()),
            getIdentifier(ctx.identifier())
        );
    }

    @Override
    public Node visitSetDimensionAliasedName(SetDimensionAliasedNameContext ctx) {
        return new SetDimensionAlias(
            getQualifiedName(ctx.tableName()),
            getAlias(ctx.setAliasedName().alias())
        );
    }

    @Override
    public Node visitChangeDimensionAttribute(ChangeDimensionAttributeContext ctx) {
        return new ChangeDimensionAttribute(
            getQualifiedName(ctx.tableName()),
            getIdentifier(ctx.old), (DimensionAttribute)visit(ctx.dimensionAttribute())
        );
    }

    @Override
    public Node visitDropDimension(DropDimensionContext ctx) {
        return new DropDimension(
            getQualifiedName(ctx.tableName()),
            ctx.ifExists() != null
        );
    }
}
