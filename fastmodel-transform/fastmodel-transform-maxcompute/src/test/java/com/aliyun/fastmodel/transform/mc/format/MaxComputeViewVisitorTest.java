/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class MaxComputeViewVisitorTest {

    @Test
    public void toSampleExpression() {
        MaxComputeContext context = MaxComputeContext.builder().build();
        MaxComputeViewVisitor maxComputeViewVisitor = new MaxComputeViewVisitor(context);
        BaseSampleDataProvider provider = maxComputeViewVisitor.getProvider();
        BaseExpression simpleData = provider.getSimpleData(new MaxComputeGenericDataType(MaxComputeDataTypeName.STRING));
        assertEquals(simpleData.toString(), "''");
    }
}