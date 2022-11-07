/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.datatype;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import com.google.auto.service.AutoService;

/**
 * https://help.aliyun.com/document_detail/130398.html
 *
 * @author panguanjing
 * @date 2022/8/14
 */
@AutoService(DataTypeConverter.class)
public class HoloToMaxComputeDataTypeConverter implements DataTypeConverter {

    private static final Map<String, IDataTypeName> KEY_DATA_TYPE_NAME = new LinkedHashMap<String, IDataTypeName>() {
        {
            put("TEXT", MaxComputeDataTypeName.STRING);
            put("INT8", MaxComputeDataTypeName.BIGINT);
            put("BIGINT", MaxComputeDataTypeName.BIGINT);
            put("INT4", MaxComputeDataTypeName.INT);
            put("INTEGER", MaxComputeDataTypeName.INT);
            put("INT", MaxComputeDataTypeName.INT);
            put("FLOAT4", MaxComputeDataTypeName.FLOAT);
            put("REAL", MaxComputeDataTypeName.FLOAT);
            put("FLOAT", MaxComputeDataTypeName.DOUBLE);
            put("FLOAT8", MaxComputeDataTypeName.DOUBLE);
            put("BOOL", MaxComputeDataTypeName.BOOLEAN);
            put("TIMESTAMP WITH TIME ZONE", MaxComputeDataTypeName.DATETIME);
            put("NUMERIC", MaxComputeDataTypeName.DECIMAL);
            put("CHAR", MaxComputeDataTypeName.CHAR);
            put("VARCHAR", MaxComputeDataTypeName.VARCHAR);
            put("DATE", MaxComputeDataTypeName.DATE);
            put("INT2", MaxComputeDataTypeName.SMALLINT);
            put("BYTEA", MaxComputeDataTypeName.BINARY);
        }
    };

    private static final Map<String, MaxComputeGenericDataType> DATA_TYPE_MAP = new LinkedHashMap<String, MaxComputeGenericDataType>() {
        {
            put("INT4[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.INT)
            )));
            put("INTEGER[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.INT)
            )));
            put("INT8[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.BIGINT)
            )));
            put("BIGINT[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.BIGINT)
            )));
            put("FLOAT4[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.FLOAT)
            )));
            put("FLOAT8[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.DOUBLE)
            )));
            put("BOOLEAN[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.BOOLEAN)
            )));
            put("TEXT[]", new MaxComputeGenericDataType(MaxComputeDataTypeName.ARRAY, new TypeParameter(
                new MaxComputeGenericDataType(MaxComputeDataTypeName.STRING)
            )));
        }
    };

    @Override
    public BaseDataType convert(BaseDataType baseDataType) {
        String typeName = baseDataType.getTypeName().getValue();
        String key = typeName.toUpperCase(Locale.ROOT);
        MaxComputeGenericDataType maxComputeGenericDataType = DATA_TYPE_MAP.get(key);
        if (maxComputeGenericDataType != null) {
            return maxComputeGenericDataType;
        }
        IDataTypeName dataTypeName = KEY_DATA_TYPE_NAME.get(key);
        if (dataTypeName != null) {
            if (baseDataType instanceof GenericDataType) {
                GenericDataType genericDataType = (GenericDataType)baseDataType;
                List<DataTypeParameter> arguments = genericDataType.getArguments();
                if (arguments == null || arguments.isEmpty()) {
                    return new MaxComputeGenericDataType(dataTypeName);
                } else {
                    return new MaxComputeGenericDataType(dataTypeName, arguments.toArray(new DataTypeParameter[0]));
                }
            } else {
                return new MaxComputeGenericDataType(dataTypeName);
            }
        }
        throw new UnsupportedOperationException("invalid typeName:" + typeName);
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.DEFAULT_HOLO;
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.DEFAULT_MAX_COMPUTE;
    }
}
