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
import com.aliyun.fastmodel.core.tree.statement.businessprocess.CreateBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.DropBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.RenameBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessAlias;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessComment;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessProperties;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.UnSetBusinessProcessProperties;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBpStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropBpStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameBpContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBpAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBpCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetBpPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetBpPropertiesContext;

import static java.util.stream.Collectors.toList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class BusinessProcessVisitor extends AstBuilder {
    @Override
    public Node visitCreateBpStatement(CreateBpStatementContext ctx) {
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        List<Property> properties = getProperties(ctx.setProperties());
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        return new CreateBusinessProcess(
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.qualifiedName()))
                .properties(properties)
                .comment(comment)
                .aliasedName(aliasedName)
                .notExists(ctx.ifNotExists() != null)
                .createOrReplace(ctx.replace() != null)
                .build()
        );
    }

    @Override
    public Node visitRenameBp(RenameBpContext ctx) {
        QualifiedName qualifiedName = getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName());
        return new RenameBusinessProcess(getQualifiedName(ctx.qualifiedName()), qualifiedName);
    }

    @Override
    public Node visitSetBpComment(SetBpCommentContext ctx) {
        Comment comment = (Comment)visit(ctx.alterStatementSuffixSetComment());
        return new SetBusinessProcessComment(getQualifiedName(ctx.qualifiedName()), comment);
    }

    @Override
    public Node visitSetBpProperties(SetBpPropertiesContext ctx) {
        List<Property> list = getProperties(ctx.setProperties());
        return new SetBusinessProcessProperties(getQualifiedName(ctx.qualifiedName()), list);
    }

    @Override
    public Node visitUnSetBpProperties(UnSetBpPropertiesContext ctx) {
        List<Identifier> list = getUnSetProperties(ctx.unSetProperties());
        return new UnSetBusinessProcessProperties(getQualifiedName(ctx.qualifiedName()),
            list.stream().map(Identifier::getValue).collect(toList())
        );
    }

    @Override
    public Node visitSetBpAliasedName(SetBpAliasedNameContext ctx) {
        return new SetBusinessProcessAlias(getQualifiedName(ctx.qualifiedName()), (AliasedName)visit(
            ctx.setAliasedName()
        ));
    }

    @Override
    public Node visitDropBpStatement(DropBpStatementContext ctx) {
        return new DropBusinessProcess(getQualifiedName(ctx.qualifiedName()));
    }
}
