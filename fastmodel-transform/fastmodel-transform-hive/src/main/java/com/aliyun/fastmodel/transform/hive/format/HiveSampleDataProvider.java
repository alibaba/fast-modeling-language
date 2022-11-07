/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.format;

import java.util.Collections;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;

/**
 * base sample
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class HiveSampleDataProvider extends BaseSampleDataProvider {
    @Override
    protected void reDefine(Map<IDataTypeName, BaseExpression> maps) {
        FunctionCall call = new FunctionCall(QualifiedName.of("current_date"), false, Collections.emptyList());
        maps.put(DataTypeEnums.DATE, call);

        FunctionCall timeStamp = new FunctionCall(QualifiedName.of("current_timestamp"), false, Collections.emptyList());
        maps.put(DataTypeEnums.TIMESTAMP, timeStamp);
    }
}
