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
import com.aliyun.fastmodel.core.tree.statement.businesscategory.CreateBusinessCategory;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.DropBusinessCategory;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.RenameBusinessCategory;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.SetBusinessCategoryAlias;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.SetBusinessCategoryComment;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.SetBusinessCategoryProperties;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.UnSetBusinessCategoryProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.market.CreateMarket;
import com.aliyun.fastmodel.core.tree.statement.market.DropMarket;
import com.aliyun.fastmodel.core.tree.statement.market.RenameMarket;
import com.aliyun.fastmodel.core.tree.statement.market.SetMarketAlias;
import com.aliyun.fastmodel.core.tree.statement.market.SetMarketComment;
import com.aliyun.fastmodel.core.tree.statement.market.SetMarketProperties;
import com.aliyun.fastmodel.core.tree.statement.market.UnSetMarketProperties;
import com.aliyun.fastmodel.core.tree.statement.subject.CreateSubject;
import com.aliyun.fastmodel.core.tree.statement.subject.DropSubject;
import com.aliyun.fastmodel.core.tree.statement.subject.RenameSubject;
import com.aliyun.fastmodel.core.tree.statement.subject.SetSubjectAlias;
import com.aliyun.fastmodel.core.tree.statement.subject.SetSubjectComment;
import com.aliyun.fastmodel.core.tree.statement.subject.SetSubjectProperties;
import com.aliyun.fastmodel.core.tree.statement.subject.UnSetSubjectProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CategoryTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBusinessCategoryStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropBusinessCategoryStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameBusinessCategoryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBusinessCategoryAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBusinessCategoryCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBusinessCategoryPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetBusinessCategoryPropertiesContext;

import static java.util.stream.Collectors.toList;

/**
 * 业务分类的visitor
 *
 * @author panguanjing
 * @date 2021/8/13
 */
@SubVisitor
public class BusinessCategoryVisitor extends AstBuilder {
    @Override
    public Node visitCreateBusinessCategoryStatement(CreateBusinessCategoryStatementContext ctx) {
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        List<Property> properties = getProperties(ctx.setProperties());
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement build = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .comment(comment)
            .aliasedName(aliasedName)
            .notExists(ctx.ifNotExists() != null)
            .createOrReplace(ctx.replace() != null)
            .build();
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new CreateSubject(build);
        } else if (byCode == StatementType.MARKET) {
            return new CreateMarket(build);
        }
        return new CreateBusinessCategory(build);
    }

    @Override
    public Node visitRenameBusinessCategory(RenameBusinessCategoryContext ctx) {
        QualifiedName qualifiedName = getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName());
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new RenameSubject(getQualifiedName(ctx.qualifiedName()), qualifiedName);
        } else if (byCode == StatementType.MARKET) {
            return new RenameMarket(getQualifiedName(ctx.qualifiedName()), qualifiedName);
        }
        return new RenameBusinessCategory(getQualifiedName(ctx.qualifiedName()), qualifiedName);
    }

    @Override
    public Node visitSetBusinessCategoryComment(SetBusinessCategoryCommentContext ctx) {
        Comment comment = (Comment)visit(ctx.alterStatementSuffixSetComment());
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new SetSubjectComment(getQualifiedName(ctx.qualifiedName()), comment);
        } else if (byCode == StatementType.MARKET) {
            return new SetMarketComment(getQualifiedName(ctx.qualifiedName()), comment);
        }
        return new SetBusinessCategoryComment(getQualifiedName(ctx.qualifiedName()), comment);
    }

    @Override
    public Node visitSetBusinessCategoryProperties(SetBusinessCategoryPropertiesContext ctx) {
        List<Property> list = getProperties(ctx.setProperties());
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new SetSubjectProperties(getQualifiedName(ctx.qualifiedName()), list);
        } else if (byCode == StatementType.MARKET) {
            return new SetMarketProperties(getQualifiedName(ctx.qualifiedName()), list);
        }
        return new SetBusinessCategoryProperties(getQualifiedName(ctx.qualifiedName()), list);
    }

    @Override
    public Node visitUnSetBusinessCategoryProperties(UnSetBusinessCategoryPropertiesContext ctx) {
        List<Identifier> list = getUnSetProperties(ctx.unSetProperties());
        List<String> collect = list.stream().map(Identifier::getValue).collect(toList());
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new UnSetSubjectProperties(getQualifiedName(ctx.qualifiedName()), collect);
        } else if (byCode == StatementType.MARKET) {
            return new UnSetMarketProperties(getQualifiedName(ctx.qualifiedName()), collect);
        }
        return new UnSetBusinessCategoryProperties(
            getQualifiedName(ctx.qualifiedName()),
            collect
        );
    }

    @Override
    public Node visitSetBusinessCategoryAliasedName(SetBusinessCategoryAliasedNameContext ctx) {
        AliasedName alias = (AliasedName)visit(
            ctx.setAliasedName()
        );
        QualifiedName qualifiedName = getQualifiedName(ctx.qualifiedName());
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        if (byCode == StatementType.SUBJECT) {
            return new SetSubjectAlias(qualifiedName, alias);
        } else if (byCode == StatementType.MARKET) {
            return new SetMarketAlias(qualifiedName, alias);
        }
        return new SetBusinessCategoryAlias(
            qualifiedName,
            alias);
    }

    @Override
    public Node visitDropBusinessCategoryStatement(DropBusinessCategoryStatementContext ctx) {
        CategoryTypeContext categoryTypeContext = ctx.categoryType();
        StatementType byCode = StatementType.getByCode(categoryTypeContext.getText());
        QualifiedName qualifiedName = getQualifiedName(ctx.qualifiedName());
        if (byCode == StatementType.SUBJECT) {
            return new DropSubject(qualifiedName);
        } else if (byCode == StatementType.MARKET) {
            return new DropMarket(qualifiedName);
        }
        return new DropBusinessCategory(qualifiedName);
    }
}
