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
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.DropDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddDqcRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeDqcRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeDqcRuleItemContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateDqcRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DqcColumnOrTableRuleListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DqcTableRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropDqcRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.PartitionSpecContext;
import com.google.common.collect.ImmutableList;

/**
 * DqcRuleVisitor
 *
 * @author panguanjing
 * @date 2021/7/21
 */
@SubVisitor
public class DqcRuleVisitor extends AstBuilder {

    @Override
    public Node visitCreateDqcRule(CreateDqcRuleContext ctx) {
        List<Property> list = ImmutableList.of();
        if (ctx.setProperties() != null) {
            list = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        CreateElement createElement =
            CreateElement.builder()
                .qualifiedName(getQualifiedName(ctx.qualifiedName()))
                .comment(visitIfPresent(ctx.comment(), Comment.class).orElse(null))
                .aliasedName(visitIfPresent(ctx.alias(), AliasedName.class).orElse(null))
                .properties(list)
                .build();
        DqcColumnOrTableRuleListContext dqcColumnOrTableRuleListContext = ctx.dqcColumnOrTableRuleList();
        List<BaseCheckElement> baseCheckElements = ImmutableList.of();
        if (dqcColumnOrTableRuleListContext != null && dqcColumnOrTableRuleListContext.dqcColumnOrTableRule() != null) {
            baseCheckElements = visit(
                dqcColumnOrTableRuleListContext.dqcColumnOrTableRule(),
                BaseCheckElement.class);
        }
        List<PartitionSpec> partitionSpecs = getPartitionSpecList(ctx.partitionSpec());
        return CreateDqcRule.builder()
            .createElement(createElement)
            .partitionSpecList(partitionSpecs)
            .tableName(getQualifiedName(ctx.tableName()))
            .ruleDefinitions(baseCheckElements)
            .build();
    }

    private List<PartitionSpec> getPartitionSpecList(PartitionSpecContext ctx) {
        List<PartitionSpec> partitionSpecs = ImmutableList.of();
        if (ctx != null) {
            partitionSpecs = visit(ctx.partitionExpression(), PartitionSpec.class);
        }
        return partitionSpecs;
    }

    @Override
    public Node visitAddDqcRule(AddDqcRuleContext ctx) {
        List<BaseCheckElement> baseCheckElements = visit(ctx.dqcColumnOrTableRuleList().dqcColumnOrTableRule(),
            BaseCheckElement.class);
        QualifiedName qualifiedName = null;
        QualifiedName tableName = null;
        if (ctx.alterDqcRuleSuffix().qualifiedName() != null) {
            qualifiedName = getQualifiedName(ctx.alterDqcRuleSuffix().qualifiedName());
        } else {
            tableName = getQualifiedName(ctx.alterDqcRuleSuffix().tableName());
        }
        return new AddDqcRule(
            qualifiedName,
            tableName,
            getPartitionSpecList(ctx.alterDqcRuleSuffix().partitionSpec()),
            baseCheckElements
        );
    }

    @Override
    public Node visitChangeDqcRule(ChangeDqcRuleContext ctx) {
        QualifiedName qualifiedName = null;
        QualifiedName tableName = null;
        if (ctx.alterDqcRuleSuffix().qualifiedName() != null) {
            qualifiedName = getQualifiedName(ctx.alterDqcRuleSuffix().qualifiedName());
        } else {
            tableName = getQualifiedName(ctx.alterDqcRuleSuffix().tableName());
        }
        return new ChangeDqcRule(
            qualifiedName,
            tableName,
            getPartitionSpecList(ctx.alterDqcRuleSuffix().partitionSpec()),
            visit(ctx.changeDqcRuleItem(), ChangeDqcRuleElement.class)
        );
    }

    @Override
    public Node visitChangeDqcRuleItem(ChangeDqcRuleItemContext ctx) {
        return new ChangeDqcRuleElement(
            (Identifier)visit(ctx.oldRule),
            (BaseCheckElement)visit(ctx.dqcColumnOrTableRule())
        );
    }

    @Override
    public Node visitDropDqcRule(DropDqcRuleContext ctx) {
        QualifiedName qualifiedName = null;
        QualifiedName tableName = null;
        if (ctx.alterDqcRuleSuffix().qualifiedName() != null) {
            qualifiedName = getQualifiedName(ctx.alterDqcRuleSuffix().qualifiedName());
        } else {
            tableName = getQualifiedName(ctx.alterDqcRuleSuffix().tableName());
        }
        return new DropDqcRule(
            qualifiedName,
            tableName,
            getPartitionSpecList(ctx.alterDqcRuleSuffix().partitionSpec()),
            (Identifier)visit(ctx.ruleName)
        );
    }

    @Override
    public Node visitDqcTableRule(DqcTableRuleContext ctx) {
        Identifier visit = (Identifier)visit(ctx.constraint);
        BaseExpression baseExpression = visitIfPresent(ctx.expression(), BaseExpression.class).orElse(null);
        return TableCheckElement.builder()
            .checkerName(visit)
            .expression(baseExpression)
            .enable(ctx.KW_DISABLE() == null)
            .enforced(ctx.enforce() != null && ctx.enforce().KW_NOT() == null)
            .build();
    }
}
