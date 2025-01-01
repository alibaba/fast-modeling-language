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

package com.aliyun.fastmodel.transform.api.domain.dqc.constant;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

/**
 * 模板定义处理
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public enum TemplateDefine {

    /**
     * 唯一值数量
     */
    UNIQUE_COUNT("SYSTEM:field:count_distinct:fixed", "字段唯一值个数期望值校验", new ColumnFunction(BaseFunctionName.UNIQUE_COUNT, null, null),
        CheckerType.FIX_STRATEGY_CHECK),

    /**
     * 空值
     */
    NULL_COUNT("SYSTEM:field:null_value:fixed", "字段空值个数", new ColumnFunction(BaseFunctionName.NULL_COUNT, null, null),
        CheckerType.FIX_STRATEGY_CHECK),

    /**
     * 重复值
     */
    DUPLICATE_COUNT("SYSTEM:field:duplicated_count:fixed", "字段重复值个数", new ColumnFunction(BaseFunctionName.DUPLICATE_COUNT, null, null),
        CheckerType.FIX_STRATEGY_CHECK),

    TABLE_SIZE_VOL_ONE_DAY("SYSTEM:table:table_size:flux:1_bizdate", "ODPS表大小，1天波动检测",
        new VolFunction(new TableFunction(BaseFunctionName.TABLE_SIZE,
            ImmutableList.of()),
            ImmutableList.of(new

                LongLiteral("1"))), CheckerType.VOL_STRATEGY_CHECK),

    /**
     * 表达式小，7天
     */
    TABLE_SIZE_VOL_SEVEN_DAY("SYSTEM:table:table_size:flux:7_bizdate", "ODPS表大小，7天波动检测",
        new VolFunction(new TableFunction(BaseFunctionName.TABLE_SIZE,
            ImmutableList.of()),
            ImmutableList.of(new

                LongLiteral("7"))), CheckerType.VOL_STRATEGY_CHECK),

    /**
     * 表行数
     */
    TABLE_COUNT("SYSTEM:table:table_count:dynamic_threshold", "表行数", new TableFunction(BaseFunctionName.TABLE_COUNT, ImmutableList.of()),
        CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 表大小
     */
    TABLE_SIZE("SYSTEM:table:table_size:dynamic_threshold", "表大小", new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
        CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 平均值
     */
    AVG("SYSTEM:field:avg:dynamic_threshold", "平均值", new ColumnFunction(BaseFunctionName.AVG, null, null), CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 汇总值
     */
    SUM("SYSTEM:field:sum:dynamic_threshold", "汇总值", new ColumnFunction(BaseFunctionName.SUM, null, null), CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 最小值
     */
    MIN("SYSTEM:field:min:dynamic_threshold", "最小值", new ColumnFunction(BaseFunctionName.MIN, null, null), CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 最大值
     */
    MAX("SYSTEM:field:max:dynamic_threshold", "最大值", new ColumnFunction(BaseFunctionName.MAX, null, null), CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 唯一值
     */
    UNIQUE_COUNT_DY("SYSTEM:field:count_distinct:fixed", "最大值", new ColumnFunction(BaseFunctionName.UNIQUE_COUNT, null, null),

        CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 离散值个数
     */
    GROUP_COUNT("SYSTEM:field:discrete_group_count:dynamic_threshold", "离散值，分组个数",
        new ColumnFunction(BaseFunctionName.DISCRETE_GROUP_COUNT, null, null),

        CheckerType.DYNAMIC_STRATEGY_CHECK),

    /**
     * 字段组合重复个数
     */
    DUPLICATE_COUNT_FIELDS("SYSTEM:fields:duplicated_count:fixed", "字段组合重复值个数", new TableFunction(BaseFunctionName.UNIQUE, null),
        CheckerType.FIX_STRATEGY_CHECK);

    // /**
    //  * 状态值
    //  */
    // STATE_COUNT(308, "离散值，状态格式", new ColumnFunction(BaseFunctionName.STATE_COUNT, null, null),
    //
    //     CheckerType.DYNAMIC_STRATEGY_CHECK);

    @Getter
    private String templateCode;

    @Getter
    private String ruleName;

    @Getter
    private BaseFunction baseFunction;

    @Getter
    private CheckerType checkerType;

    private TemplateDefine(String templateCode, String ruleName, BaseFunction baseFunction, CheckerType checkerType) {
        this.templateCode = templateCode;
        this.ruleName = ruleName;
        this.baseFunction = baseFunction;
        this.checkerType = checkerType;
    }

    public static TemplateDefine getTemplateIdByFunction(BaseFunction baseFunction,
        CheckerType checkerType) {
        TemplateDefine[] templateDefines = TemplateDefine.values();
        for (TemplateDefine templateDefine : templateDefines) {
            if (checkerType != templateDefine.checkerType) {
                continue;
            }
            BaseFunction function = templateDefine.getBaseFunction();
            BaseFunctionName baseFunctionName = function.funcName();
            if (function.funcName() != baseFunction.funcName()) {
                continue;
            }
            //if function is volFunction
            if (baseFunction instanceof VolFunction) {
                VolFunction volFunction = (VolFunction)baseFunction;
                VolFunction defineFunction = (VolFunction)function;
                if (volFunction.getBaseFunction().funcName() != defineFunction.getBaseFunction().funcName()) {
                    continue;
                }
                List<BaseExpression> dateList = volFunction.getDateList();
                boolean equalCollection = CollectionUtils.isEqualCollection(dateList, defineFunction.getDateList());
                if (!equalCollection) {
                    continue;
                }
                return templateDefine;
            }
            return templateDefine;
        }
        throw new IllegalArgumentException("can't find the templateId with baseFunction:" + baseFunction.getClass());
    }
}
