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

package com.aliyun.fastmodel.core.tree.statement.rule.strategy;

import java.util.List;
import java.util.Locale;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy.DYNMIAC;
import static com.google.common.collect.Iterables.isEmpty;

/**
 * ruleStrategy util
 *
 * @author panguanjing
 * @date 2021/7/21
 */
public class StrategyUtil {

    public static final int WITHOUT_COLUMN_LENGTH = 2;
    public static final int WITH_COLUMN_LENGTH = 3;
    public static final int TABLE_INDEX = 1;
    public static final int MATCH_COLUMN_INDEX = 2;

    /**
     * 转换表达式
     *
     * @param baseExpression
     * @return {@see RuleStrategy}
     */
    public static RuleStrategy toStrategy(BaseExpression baseExpression) {
        if (baseExpression instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression)baseExpression;
            BaseExpression left = comparisonExpression.getLeft();
            return new FixedStrategy(
                toFixedFunction(left, null),
                comparisonExpression.getOperator(),
                (BaseLiteral)comparisonExpression.getRight()
            );
        } else if (baseExpression instanceof FunctionCall) {
            FunctionCall functionCall = (FunctionCall)baseExpression;
            if (functionCall.getFuncName().equals(DYNMIAC)) {
                List<BaseExpression> arguments = functionCall.getArguments();
                return new DynamicStrategy(toFixedFunction(arguments.get(0), null), (BaseLiteral)arguments.get(1));
            }
        } else if (baseExpression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression arithmeticBinaryExpression = (ArithmeticBinaryExpression)baseExpression;
            BaseExpression left = arithmeticBinaryExpression.getLeft();
            return new VolStrategy(
                toVolFunction(left, null),
                VolOperator.toVolOperator(arithmeticBinaryExpression.getOperator()),
                (VolInterval)arithmeticBinaryExpression.getRight()
            );
        }
        return null;
    }

    public static BaseFunction toFixedFunction(BaseExpression expression,
        BaseDataType baseDataType) {
        if (expression instanceof BaseFunction) {
            return (BaseFunction)expression;
        }
        if (!(expression instanceof FunctionCall)) {
            return null;
        }
        FunctionCall functionCall = (FunctionCall)expression;
        QualifiedName funcName = functionCall.getFuncName();
        String suffix = funcName.getSuffix();
        try {
            BaseFunctionName baseFunctionName = BaseFunctionName.valueOf(suffix.toUpperCase(Locale.ROOT));
            if (baseFunctionName.isTableFunction()) {
                return new TableFunction(baseFunctionName, functionCall.getArguments());
            } else {
                List<BaseExpression> arguments = functionCall.getArguments();
                if (isEmpty(arguments)) {
                    return null;
                }
                BaseExpression baseExpression = arguments.get(0);
                if (!(baseExpression instanceof TableOrColumn)) {
                    return null;
                }
                TableOrColumn tableOrColumn = (TableOrColumn)baseExpression;
                if (baseFunctionName == BaseFunctionName.IN_TABLE) {
                    if (arguments.size() < WITHOUT_COLUMN_LENGTH || arguments.size() > WITH_COLUMN_LENGTH) {
                        //非法的格式
                        return null;
                    }
                    if (arguments.size() == WITHOUT_COLUMN_LENGTH) {
                        TableOrColumn tableOrColumn1 = (TableOrColumn)arguments.get(1);
                        return new InTableFunction(tableOrColumn.getQualifiedName(), tableOrColumn1.getQualifiedName(),
                            null);
                    } else if (arguments.size() == WITH_COLUMN_LENGTH) {
                        TableOrColumn tableOrColumn1 = (TableOrColumn)arguments.get(TABLE_INDEX);
                        TableOrColumn tableOrColumn2 = (TableOrColumn)arguments.get(MATCH_COLUMN_INDEX);
                        return new InTableFunction(tableOrColumn.getQualifiedName(), tableOrColumn1.getQualifiedName(),
                            new Identifier(tableOrColumn2.getQualifiedName().getSuffix()));
                    }
                }
                ColumnFunction columnFunction = new ColumnFunction(
                    baseFunctionName,
                    tableOrColumn, baseDataType);
                return columnFunction;
            }
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static VolFunction toVolFunction(BaseExpression expression, BaseDataType baseDataType) {
        if (expression instanceof VolFunction) {
            return (VolFunction)expression;
        }
        if (!(expression instanceof FunctionCall)) {
            return null;
        }
        FunctionCall functionCall = (FunctionCall)expression;
        QualifiedName funcName = functionCall.getFuncName();
        String funcNameSuffix = funcName.getSuffix();
        if (!StringUtils.equalsIgnoreCase(funcNameSuffix, BaseFunctionName.VOL.name())) {
            return null;
        }
        List<BaseExpression> arguments = functionCall.getArguments();
        if (isEmpty(arguments)) {
            return null;
        }
        BaseExpression baseExpression = arguments.get(0);
        if (!(baseExpression instanceof FunctionCall)) {
            return null;
        }
        FunctionCall functionCall1 = (FunctionCall)baseExpression;
        BaseFunction baseFunction = toFixedFunction(functionCall1, baseDataType);
        if (baseFunction == null) {
            return null;
        }
        VolFunction volFunction = new VolFunction(
            baseFunction,
            arguments.subList(1, arguments.size())
        );
        return volFunction;
    }
}
