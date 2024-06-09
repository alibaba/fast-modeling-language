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
import java.util.Locale;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRuleElement;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRules;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleGrade;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.StrategyUtil;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolOperator;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterRuleSuffixContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeRuleItemContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnOrTableRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ComparisonOperatorContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateRulesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropRuleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DynamicStrategyContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FixedStrategyContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IntervalVolContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.PartitionSpecContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QualifiedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.VolOperatorContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.VolStrategyContext;
import com.google.common.collect.ImmutableList;

/**
 * 针对Rule的visitor的处理
 *
 * @author panguanjing
 * @date 2021/5/30
 */
@SubVisitor
public class RuleVisitor extends AstBuilder {
    @Override
    public Node visitCreateRules(CreateRulesContext ctx) {
        RulesLevel rulesLevel = RulesLevel.SQL;
        if (ctx.ruleLevel() != null) {
            rulesLevel = RulesLevel.valueOf(ctx.ruleLevel().getText().toUpperCase(Locale.ROOT));
        }
        List<Property> list = ImmutableList.of();
        if (ctx.setProperties() != null) {
            list = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        CreateElement createElement = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .comment(visitIfPresent(ctx.comment(), Comment.class).orElse(null))
            .properties(list)
            .notExists(ctx.ifNotExists() != null)
            .aliasedName(visitIfPresent(ctx.alias(), AliasedName.class).orElse(null))
            .createOrReplace(ctx.replace() != null)
            .build();

        List<RuleDefinition> ruleDefinitions = visit(ctx.columnOrTableRuleList().columnOrTableRule(),
            RuleDefinition.class);

        List<PartitionSpec> partitionSpecList = ImmutableList.of();
        if (ctx.partitionSpec() != null) {
            partitionSpecList = visit(ctx.partitionSpec().partitionExpression(), PartitionSpec.class);
        }
        return CreateRules.builder().ruleLevel(rulesLevel).tableName(getQualifiedName(ctx.tableName())).createElement(
            createElement).ruleDefinitions(ruleDefinitions).build();
    }

    @Override
    public Node visitAddRule(AddRuleContext ctx) {
        AlterRuleSuffixContext alterRuleSuffixContext = ctx.alterRuleSuffix();
        if (alterRuleSuffixContext.qualifiedName() != null) {
            return new AddRules(
                getQualifiedName(alterRuleSuffixContext.qualifiedName()),
                visit(ctx.columnOrTableRuleList().columnOrTableRule(), RuleDefinition.class)
            );
        } else if (alterRuleSuffixContext.tableName() != null) {
            PartitionSpecContext partitionSpecContext = ctx.alterRuleSuffix().partitionSpec();
            List<PartitionSpec> visit = ImmutableList.of();
            if (partitionSpecContext != null) {
                visit = visit(partitionSpecContext.partitionExpression(),
                    PartitionSpec.class);
            }
            return new AddRules(getQualifiedName(ctx.alterRuleSuffix().tableName()),
                visit,
                visit(ctx.columnOrTableRuleList().columnOrTableRule(), RuleDefinition.class)
            );
        }
        return null;
    }

    @Override
    public Node visitChangeRule(ChangeRuleContext ctx) {
        AlterRuleSuffixContext alterRuleSuffixContext = ctx.alterRuleSuffix();
        if (alterRuleSuffixContext.qualifiedName() != null) {
            return new ChangeRules(
                getQualifiedName(alterRuleSuffixContext.qualifiedName()),
                visit(ctx.changeRuleItem(), ChangeRuleElement.class)
            );
        } else if (alterRuleSuffixContext.tableName() != null) {
            PartitionSpecContext partitionSpecContext = ctx.alterRuleSuffix().partitionSpec();
            List<PartitionSpec> visit = ImmutableList.of();
            if (partitionSpecContext != null) {
                visit = visit(partitionSpecContext.partitionExpression(),
                    PartitionSpec.class);
            }
            return new ChangeRules(getQualifiedName(ctx.alterRuleSuffix().tableName()),
                visit,
                visit(ctx.changeRuleItem(), ChangeRuleElement.class)
            );
        }
        return null;
    }

    @Override
    public Node visitChangeRuleItem(ChangeRuleItemContext ctx) {
        ChangeRuleElement ruleElement = new ChangeRuleElement(
            (Identifier)visit(ctx.identifier()),
            visitIfPresent(ctx.columnOrTableRule(), RuleDefinition.class).get()
        );
        return ruleElement;
    }

    @Override
    public Node visitColumnOrTableRule(ColumnOrTableRuleContext ctx) {
        RuleGrade ruleGrade = RuleGrade.WEAK;
        if (ctx.ruleGrade() != null) {
            ruleGrade = RuleGrade.valueOf(ctx.ruleGrade().getText().toUpperCase(Locale.ROOT));
        }
        Identifier identifier = (Identifier)visit(ctx.identifier());
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        RuleStrategy ruleStrategy = visitIfPresent(ctx.strategy(), RuleStrategy.class).orElse(null);
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        boolean enable = ctx.KW_DISABLE() == null;
        RuleDefinition ruleDefinition = RuleDefinition.builder().ruleGrade(ruleGrade).ruleName(identifier).aliasedName(
                aliasedName)
            .ruleStrategy(ruleStrategy)
            .comment(comment)
            .enable(enable)
            .build();
        return ruleDefinition;
    }

    @Override
    public Node visitFixedStrategy(FixedStrategyContext ctx) {
        ComparisonOperatorContext comparisonOperatorContext = ctx.comparisonOperator();
        ComparisonOperator comparisonOperator = null;
        if (comparisonOperatorContext != null) {
            comparisonOperator = ComparisonOperator.getByCode(comparisonOperatorContext.getText());
        }

        BaseLiteral baseLiteral = visitIfPresent(ctx.numberLiteral(), BaseLiteral.class).orElse(null);
        FunctionCall functionCall = (FunctionCall)visit(ctx.functionExpression());
        BaseDataType o = visitIfPresent(ctx.colType(), BaseDataType.class).orElse(null);
        BaseFunction baseFunction = StrategyUtil.toFixedFunction(functionCall, o);
        if (baseFunction == null) {
            NodeLocation location = ParserHelper.getLocation(ctx.functionExpression());
            throw new ParseException("function is not Fixed", location.getLine(), location.getColumn());
        }
        FixedStrategy fixedStrategy = new FixedStrategy(
            baseFunction, comparisonOperator,
            baseLiteral
        );
        return fixedStrategy;
    }

    @Override
    public Node visitVolStrategy(VolStrategyContext ctx) {
        VolOperatorContext volOperatorContext = ctx.volOperator();
        FunctionCall functionCall = (FunctionCall)visit(ctx.functionExpression());
        BaseDataType dataType = visitIfPresent(ctx.colType(), BaseDataType.class).orElse(null);
        VolFunction function = StrategyUtil.toVolFunction(functionCall, dataType);
        if (function == null) {
            NodeLocation location = ParserHelper.getLocation(ctx.functionExpression());
            throw new ParseException("function is not vol", location.getLine(), location.getColumn());
        }
        VolInterval volInterval = (VolInterval)visit(ctx.intervalVol());
        VolOperator volOperator = VolOperator.ABS;
        if (volOperatorContext != null) {
            volOperator = VolOperator.valueOf(volOperatorContext.getText().toUpperCase(Locale.ROOT));
        }
        VolStrategy volStrategy = new VolStrategy(
            function,
            volOperator,
            volInterval
        );
        return volStrategy;
    }

    @Override
    public Node visitDynamicStrategy(DynamicStrategyContext ctx) {
        FunctionCall functionCall = (FunctionCall)visit(ctx.functionExpression());
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.numberLiteral());
        BaseDataType baseDataType = visitIfPresent(ctx.colType(), BaseDataType.class).orElse(null);
        DynamicStrategy dynamicStrategy = new DynamicStrategy(
            StrategyUtil.toFixedFunction(functionCall, baseDataType), baseLiteral
        );
        return dynamicStrategy;
    }

    @Override
    public Node visitIntervalVol(IntervalVolContext ctx) {
        VolInterval volInterval = new VolInterval(
            ParserHelper.getNumber((BaseLiteral)visit(ctx.low)),
            ParserHelper.getNumber((BaseLiteral)visit(ctx.high))
        );
        return volInterval;
    }

    @Override
    public Node visitDropRule(DropRuleContext ctx) {
        QualifiedNameContext qualifiedNameContext = ctx.alterRuleSuffix().qualifiedName();
        RulesLevel rulesLevel = null;
        if (ctx.ruleLevel() != null) {
            rulesLevel = RulesLevel.valueOf(ctx.ruleLevel().getText().toUpperCase(Locale.ROOT));
        }
        if (qualifiedNameContext != null) {
            return new DropRule(
                rulesLevel,
                getQualifiedName(qualifiedNameContext),
                (Identifier)visit(ctx.identifier()));
        } else {
            List<PartitionSpec> list = ImmutableList.of();
            if (ctx.alterRuleSuffix().partitionSpec() != null) {
                list = visit(ctx.alterRuleSuffix().partitionSpec().partitionExpression(), PartitionSpec.class);
            }
            return new DropRule(
                rulesLevel, getQualifiedName(ctx.alterRuleSuffix().tableName()),
                list,
                (Identifier)visit(ctx.identifier())
            );
        }
    }

}
