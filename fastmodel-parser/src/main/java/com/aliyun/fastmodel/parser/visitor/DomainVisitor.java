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
import com.aliyun.fastmodel.core.tree.statement.domain.CreateDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.DropDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.RenameDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainAlias;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainComment;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.domain.UnSetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateDomainStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropDomainStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameDomainContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDomainAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDomainCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetDomainPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetDomainPropertiesContext;
import com.google.common.collect.ImmutableList;

import static java.util.stream.Collectors.toList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class DomainVisitor extends AstBuilder {

    @Override
    public Node visitCreateDomainStatement(CreateDomainStatementContext ctx) {
        List<Property> properties = ImmutableList.of();
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        return new CreateDomain(
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.qualifiedName()))
                .notExists(ctx.ifNotExists() != null)
                .properties(properties).comment(comment)
                .aliasedName(aliasedName)
                .createOrReplace(ctx.replace() != null)
                .build());
    }

    @Override
    public Node visitRenameDomain(RenameDomainContext ctx) {
        QualifiedName qualifiedName = getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName());
        QualifiedName sourceQualified = getQualifiedName(ctx.qualifiedName());
        return new RenameDomain(sourceQualified, qualifiedName);
    }

    @Override
    public Node visitSetDomainComment(SetDomainCommentContext ctx) {
        Comment comment = (Comment)visit(ctx.alterStatementSuffixSetComment());
        return new SetDomainComment(getQualifiedName(ctx.qualifiedName()), comment);
    }

    @Override
    public Node visitSetDomainProperties(SetDomainPropertiesContext ctx) {
        return new SetDomainProperties(getQualifiedName(ctx.qualifiedName()),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitUnSetDomainProperties(UnSetDomainPropertiesContext ctx) {
        List<Identifier> list = getUnSetProperties(ctx.unSetProperties());
        return new UnSetDomainProperties(getQualifiedName(ctx.qualifiedName()),
            list.stream().map(Identifier::getValue).collect(toList())
        );
    }

    @Override
    public Node visitDropDomainStatement(DropDomainStatementContext ctx) {
        return new DropDomain(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitSetDomainAliasedName(SetDomainAliasedNameContext ctx) {
        return new SetDomainAlias(getQualifiedName(ctx.qualifiedName()), (AliasedName)visit(ctx.setAliasedName()));
    }
}
