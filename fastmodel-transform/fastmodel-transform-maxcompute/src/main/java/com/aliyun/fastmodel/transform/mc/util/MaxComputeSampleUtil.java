/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.util;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.ARRAY;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.BIGINT;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.BOOLEAN;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.DATE;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.DATETIME;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.DECIMAL;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.DOUBLE;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.FLOAT;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.INT;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.MAP;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.SMALLINT;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.STRUCT;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.TIMESTAMP;
import static com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName.TINYINT;

/**
 * sample util
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class MaxComputeSampleUtil extends BaseSampleDataProvider {

    @Override
    public BaseExpression getSimpleData(BaseDataType baseDataType) {
        return simpleSampleData(baseDataType);
    }

    /**
     * simple sample data
     *
     * @param baseDataType
     * @return {@link BaseExpression}
     */
    public static BaseExpression simpleSampleData(BaseDataType baseDataType) {
        IDataTypeName typeName = baseDataType.getTypeName();
        MaxComputeDataTypeName maxComputeDataTypeName = MaxComputeDataTypeName.getByValue(typeName.getValue());
        if (MaxComputeDataTypeName.STRING.equals(maxComputeDataTypeName) || MaxComputeDataTypeName.VARCHAR.equals(maxComputeDataTypeName)
            || MaxComputeDataTypeName.CHAR.equals(
            maxComputeDataTypeName)) {
            return new StringLiteral(StringUtils.EMPTY);
        } else if (BIGINT.equals(maxComputeDataTypeName) || TINYINT.equals(maxComputeDataTypeName) || SMALLINT.equals(maxComputeDataTypeName)
            || INT.equals(
            maxComputeDataTypeName)) {
            return new LongLiteral("0");
        } else if (FLOAT.equals(maxComputeDataTypeName) || DOUBLE.equals(maxComputeDataTypeName) || DECIMAL.equals(maxComputeDataTypeName)) {
            return new DecimalLiteral("0.0");
        } else if (DATE.equals(maxComputeDataTypeName) || DATETIME.equals(maxComputeDataTypeName) || TIMESTAMP.equals(maxComputeDataTypeName)) {
            //日期的类，统一使用GETDATE函数返回
            return new FunctionCall(QualifiedName.of("GETDATE"), false, Collections.emptyList());
        } else if (BOOLEAN.equals(maxComputeDataTypeName)) {
            return new BooleanLiteral(Boolean.TRUE.toString());
        } else if (ARRAY.equals(maxComputeDataTypeName)) {
            return getFunctionCall((GenericDataType)baseDataType, ARRAY.name());
        } else if (STRUCT.equals(maxComputeDataTypeName)) {
            return getFunctionCall((GenericDataType)baseDataType, STRUCT.name());
        } else if (MAP.equals(maxComputeDataTypeName)) {
            return getFunctionCall((GenericDataType)baseDataType, MAP.name());
        }
        return new NullLiteral(null, null);
    }

    private static FunctionCall getFunctionCall(GenericDataType baseDataType, String functionName) {
        List<BaseExpression> values = Lists.newArrayList();
        List<DataTypeParameter> arguments = baseDataType.getArguments();
        for (DataTypeParameter a : arguments) {
            if (a instanceof TypeParameter) {
                TypeParameter numericParameter = (TypeParameter)a;
                BaseExpression baseExpression = simpleSampleData(numericParameter.getType());
                values.add(baseExpression);
            }
        }
        return QueryUtil.functionCall(functionName, values.toArray(new BaseExpression[0]));
    }
}
