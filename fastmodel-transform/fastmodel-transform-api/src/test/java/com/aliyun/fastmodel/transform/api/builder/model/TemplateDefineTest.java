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

package com.aliyun.fastmodel.transform.api.builder.model;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.CheckerType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.TemplateDefine;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class TemplateDefineTest {

    private BaseDataType baseDataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);

    @Test
    public void getTemplateIdByFunction() {
        TemplateDefine templateIdByFunction = TemplateDefine.getTemplateIdByFunction(
            new ColumnFunction(BaseFunctionName.UNIQUE_COUNT, new TableOrColumn(
                QualifiedName.of("a.b")), baseDataType), CheckerType.FIX_STRATEGY_CHECK);
        assertEquals(templateIdByFunction.getTemplateId(), Integer.valueOf(5));

        templateIdByFunction = TemplateDefine.getTemplateIdByFunction(
            new ColumnFunction(BaseFunctionName.UNIQUE_COUNT, new TableOrColumn(
                QualifiedName.of("a.b")), baseDataType), CheckerType.DYNAMIC_STRATEGY_CHECK);
        assertEquals(templateIdByFunction.getTemplateId(), Integer.valueOf(306));
    }

    @Test
    public void getTemplateIdByFunction2() {
        TemplateDefine templateIdByFunction = TemplateDefine.getTemplateIdByFunction(
            new ColumnFunction(BaseFunctionName.NULL_COUNT, new TableOrColumn(
                QualifiedName.of("a.b")), DataTypeUtil.simpleType(DataTypeEnums.BIGINT)),
            CheckerType.FIX_STRATEGY_CHECK);
        assertEquals(templateIdByFunction.getTemplateId(), Integer.valueOf(11));
    }

    @Test
    public void testGetTemplateIdByVol() {
        TemplateDefine templateIdByFunction = TemplateDefine.getTemplateIdByFunction(
            new VolFunction(new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ImmutableList.of(new LongLiteral("1"), new LongLiteral("7"), new LongLiteral("30"))),
            CheckerType.VOL_STRATEGY_CHECK);
        assertEquals(Integer.valueOf(33), templateIdByFunction.getTemplateId());
    }
}