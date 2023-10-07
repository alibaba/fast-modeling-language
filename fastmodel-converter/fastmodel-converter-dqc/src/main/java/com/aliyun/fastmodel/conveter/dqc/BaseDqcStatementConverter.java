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

package com.aliyun.fastmodel.conveter.dqc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.converter.spi.BaseStatementConverter;
import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.conveter.dqc.util.FmlTableUtil;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleGrade;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * 提供一些公用的方法内容处理
 *
 * @author panguanjing
 * @date 2021/6/2
 */
public abstract class BaseDqcStatementConverter<T extends BaseStatement, R extends BaseStatement>
    extends BaseStatementConverter<T, R, ConvertContext> {

    public static final String EXPECT_ZERO = "0";
    public static final String COLON = ":";

    /**
     * 重复的内容去重
     *
     * @param keyExtractor
     * @param <T>
     * @return
     */
    protected <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>(16);
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    public List<RuleDefinition> toRuleDefinition(CreateTable source, boolean enable, ConvertContext context) {
        List<RuleDefinition> ruleDefinitions = new ArrayList<>();
        /**
         * 列上的约束，主要根据是否主键，以及数据字典的约束
         * 默认都是弱规则，用于提醒，未来需要支持传入context参数处理
         */
        Map<Identifier, ColumnDefinition> map = new HashMap<>(10);
        if (!source.isColumnEmpty()) {
            List<ColumnDefinition> columnDefines = source.getColumnDefines();
            for (ColumnDefinition c : columnDefines) {
                map.put(c.getColName(), c);
            }
            List<RuleDefinition> list = toRuleDefinition(columnDefines, enable, context);
            if (list != null) {
                ruleDefinitions.addAll(list);
            }
        }
        if (!source.isPartitionEmpty()) {
            List<ColumnDefinition> columnDefinitions = source.getPartitionedBy().getColumnDefinitions();
            for (ColumnDefinition c : columnDefinitions) {
                map.put(c.getColName(), c);
            }
            List<RuleDefinition> list = toRuleDefinition(columnDefinitions, enable, context);
            if (list != null) {
                ruleDefinitions.addAll(list);
            }
        }
        if (!source.isConstraintEmpty()) {
            List<BaseConstraint> constraintStatements = source.getConstraintStatements();
            for (BaseConstraint constraint : constraintStatements) {
                if (!(constraint instanceof PrimaryConstraint)) {
                    continue;
                }
                PrimaryConstraint c = (PrimaryConstraint)constraint;
                if (c.getColNames().size() == 1) {
                    ColumnDefinition columnDefinition = map.get(c.getColNames().get(0));
                    List<RuleDefinition> list = toPrimaryRule(columnDefinition, enable);
                    ruleDefinitions.addAll(list);
                } else {
                    RuleDefinition ruleDefinition = toUniqueRule(source.getQualifiedName().getSuffix(), c.getColNames(),
                        enable);
                    ruleDefinitions.add(ruleDefinition);
                    List<RuleDefinition> notNullRule = c.getColNames().stream().map(x -> {
                        ColumnDefinition columnDefinition = ColumnDefinition.builder().colName(x).notNull(true).build();
                        return toNotNullRule(columnDefinition, enable);
                    }).collect(Collectors.toList());
                    ruleDefinitions.addAll(notNullRule);
                }
            }
        }

        return ruleDefinitions;
    }

    /**
     * 将列信息，转换为规则定义信息内容
     *
     * @param oldColumnName
     * @param columnDefines
     * @param enable
     * @param context
     * @return
     */
    public List<RuleDefinition> toRuleDefinition(List<ColumnDefinition> columnDefines, Boolean enable, ConvertContext context) {
        List<RuleDefinition> ruleDefinitions = new ArrayList<>();
        for (ColumnDefinition c : columnDefines) {
            Boolean primary = c.getPrimary();
            if (primary != null) {
                List<RuleDefinition> definitions = toPrimaryRule(c, primary);
                if (!enable) {
                    definitions = toPrimaryRule(c, enable);
                }
                if (definitions != null) {
                    ruleDefinitions.addAll(definitions);
                }
            }
            Boolean notNull = c.getNotNull();
            if (notNull != null) {
                RuleDefinition definitions = toNotNullRule(c, notNull);
                if (!enable) {
                    definitions = toNotNullRule(c, enable);
                }
                if (definitions != null) {
                    ruleDefinitions.add(definitions);
                }
            }
            List<RuleDefinition> codeTableRules = codeTableRule(c, context, enable);
            ruleDefinitions.addAll(codeTableRules);
        }
        return ruleDefinitions.stream().filter(distinctByKey(definition -> {
            return getDistinctKey(definition);
        })).collect(Collectors.toList());
    }

    protected String getDistinctKey(RuleDefinition definition) {
        return definition.getRuleName() + COLON + definition.isEnable();
    }

    private List<RuleDefinition> codeTableRule(ColumnDefinition c, ConvertContext context, boolean enable) {
        List<RuleDefinition> ruleDefinitions = Lists.newArrayList();
        Optional<Property> property = codeFound(c);
        if (!property.isPresent()) {return ruleDefinitions;}
        boolean codeEnable = StringUtils.isNotBlank(property.get().getValue());

        //如果codeEnable, 那么还要看之前的是否存在，存在的话，生成一个删除的规则定义, 顺序是先删除，后添加
        RuleDefinition deleteRule = generatorDeleteRule(c, context, codeEnable);
        if (deleteRule != null) {
            ruleDefinitions.add(deleteRule);
        }
        //如果不是enable，只生成删除的规则的操作
        if (!enable) {
            return ruleDefinitions;
        }
        RuleDefinition definition = toCodeRule(c, codeEnable);
        if (definition != null) {
            ruleDefinitions.add(definition);
        }
        return ruleDefinitions;
    }

    private RuleDefinition generatorDeleteRule(ColumnDefinition c, ConvertContext context, boolean codeEnable) {
        if (!codeEnable) {
            return null;
        }
        boolean createTable = context != null && context.getBeforeStatement() instanceof CreateTable;
        if (!createTable) {
            return null;
        }
        CreateTable beforeStatement = (CreateTable)context.getBeforeStatement();
        ColumnDefinition sourceColumn = beforeStatement.getColumn(c.getColName());
        if (!codeFound(sourceColumn).isPresent()) {
            return null;
        }
        return toCodeRule(sourceColumn, false);
    }

    protected BaseCheckElement toSingleCheckElement(RuleDefinition build) {
        return toCheckElement(ImmutableList.of(build)).get(0);
    }

    protected List<BaseCheckElement> toCheckElement(List<RuleDefinition> list) {
        return list.stream().map(x -> {
            TableCheckElement columnCheckElement = TableCheckElement.builder()
                .checkerName(x.getRuleName())
                .enable(x.isEnable())
                .enforced(x.getRuleGrade() == RuleGrade.STRONG)
                .expression(x.getRuleStrategy() == null ? null : x.getRuleStrategy().toExpression())
                .build();
            return columnCheckElement;
        }).collect(Collectors.toList());
    }

    public RuleDefinition toUniqueRule(String tableName, List<Identifier> colNames, Boolean enable) {
        return RuleDefinition.builder().enable(enable).
            ruleName(RuleUtil.generateRuleNameByFunction(BaseFunctionName.UNIQUE, colNames.toArray(new Identifier[0])))
            .ruleGrade(RuleGrade.WEAK).ruleStrategy(
                new FixedStrategy(
                    new TableFunction(BaseFunctionName.UNIQUE, colNames.stream().map(x -> {
                        return x;
                    }).collect(Collectors.toList())),
                    ComparisonOperator.EQUAL,
                    new LongLiteral(EXPECT_ZERO)
                )
            ).build();
    }

    /**
     * 是否关联标准代码
     *
     * @param c
     * @return
     */
    protected Optional<Property> codeFound(ColumnDefinition c) {
        if (c == null) {
            return Optional.empty();
        }
        boolean propertyEmpty = c.isPropertyEmpty();
        if (propertyEmpty) {
            return Optional.empty();
        }
        return c.getColumnProperties().stream().filter(x -> x.getName().equals(
            ColumnPropertyDefaultKey.code_table.name())).findFirst();
    }

    /**
     * 数据标准
     *
     * @param c
     * @param enable
     * @return
     */
    protected RuleDefinition toCodeRule(ColumnDefinition c, Boolean enable) {
        QualifiedName of = QualifiedName.of(c.getColName().getValue());
        TableOrColumn tableOrColumn = new TableOrColumn(of);
        Optional<Property> first = c.getColumnProperties().stream().filter(x -> x.getName().equals(
            ColumnPropertyDefaultKey.code_table.name())).findFirst();
        String value = first.get().getValue();
        InTableFunction inTableFunction = new InTableFunction(
            of,
            QualifiedName.of(value),
            null
        );
        FixedStrategy fixedStrategy = new FixedStrategy(inTableFunction, ComparisonOperator.EQUAL,
            new LongLiteral(EXPECT_ZERO));
        return RuleDefinition.builder().ruleGrade(RuleGrade.WEAK).ruleStrategy(fixedStrategy).ruleName(
            RuleUtil.generateRuleNameByFunction(BaseFunctionName.IN_TABLE, c.getColName())).enable(enable).build();
    }

    /**
     * 非空规则
     *
     * @param c
     * @param enable
     * @return
     */
    protected RuleDefinition toNotNullRule(ColumnDefinition c, Boolean enable) {
        TableOrColumn tableOrColumn = new TableOrColumn(QualifiedName.of(c.getColName().getValue()));
        FixedStrategy ruleStrategy = new FixedStrategy(new ColumnFunction(BaseFunctionName.NULL_COUNT, tableOrColumn,
            c.getDataType()),
            ComparisonOperator.EQUAL, new LongLiteral(EXPECT_ZERO));
        return RuleDefinition.builder().enable(enable).ruleStrategy(
            ruleStrategy
        ).ruleGrade(RuleGrade.WEAK).ruleName(
            RuleUtil.generateRuleNameByFunction(BaseFunctionName.NULL_COUNT, c.getColName())).build();
    }

    /**
     * 主键规则
     *
     * @param c
     * @param enable
     * @return
     */
    protected List<RuleDefinition> toPrimaryRule(ColumnDefinition c, boolean enable) {
        List<RuleDefinition> list = new ArrayList<>();
        TableOrColumn tableOrColumn = new TableOrColumn(QualifiedName.of(c.getColName().getValue()));
        ColumnFunction columnFunction = new ColumnFunction(BaseFunctionName.DUPLICATE_COUNT, tableOrColumn,
            c.getDataType());
        LongLiteral expectValue = new LongLiteral(EXPECT_ZERO);
        FixedStrategy fixedStrategy = new FixedStrategy(columnFunction, ComparisonOperator.EQUAL, expectValue);
        RuleDefinition duplicateCountZero = RuleDefinition.builder().enable(enable).ruleStrategy(
                fixedStrategy
            ).ruleGrade(RuleGrade.WEAK).ruleName(
                RuleUtil.generateRuleNameByFunction(BaseFunctionName.DUPLICATE_COUNT, c.getColName()))
            .build();
        ;
        list.add(duplicateCountZero);
        return list;
    }

    public List<PartitionSpec> getPartitionSpec(ConvertContext context) {
        Preconditions.checkNotNull(context, "context can't be null");
        BaseStatement afterStatement = context.getAfterStatement();
        assert afterStatement instanceof CreateTable;
        CreateTable createTable = (CreateTable)afterStatement;
        return FmlTableUtil.getPartitionSpec(createTable);
    }

    public List<PartitionSpec> getPartitionSpec(CreateTable createTable) {
        Preconditions.checkNotNull(createTable, "createTable can't be null");
        return FmlTableUtil.getPartitionSpec(createTable);
    }
}
