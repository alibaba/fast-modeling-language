/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.format;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.BIGINT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.BOOLEAN;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.CHAR;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.DATE;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.DATETIME;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.DECIMAL;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.DOUBLE;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.FLOAT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.INT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.JSON;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.MEDIUMINT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.SMALLINT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.STRING;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.TEXT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.TIMESTAMP;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.TINYINT;
import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.VARCHAR;

/**
 * BaseSampleData
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class BaseSampleDataProvider {

    protected final Map<IDataTypeName, BaseExpression> maps = Maps.newLinkedHashMap();

    public BaseSampleDataProvider() {
        initDefault();
    }

    public static BaseSampleDataProvider getInstance() {
        return new BaseSampleDataProvider();
    }

    protected void initDefault() {
        StringLiteral stringLiteral = new StringLiteral(StringUtils.EMPTY);
        maps.put(STRING, stringLiteral);
        maps.put(VARCHAR, stringLiteral);
        maps.put(CHAR, stringLiteral);
        maps.put(TEXT, stringLiteral);
        maps.put(JSON, stringLiteral);
        //numeric
        LongLiteral longLiteral = new LongLiteral("0");
        maps.put(BIGINT, longLiteral);
        maps.put(TINYINT, longLiteral);
        maps.put(SMALLINT, longLiteral);
        maps.put(MEDIUMINT, longLiteral);
        maps.put(INT, longLiteral);

        //decimal
        DecimalLiteral decimalLiteral = new DecimalLiteral("0.0");
        maps.put(FLOAT, decimalLiteral);
        maps.put(DECIMAL, decimalLiteral);
        maps.put(DOUBLE, decimalLiteral);

        //boolean
        BooleanLiteral booleanLiteral = new BooleanLiteral(Boolean.TRUE.toString());
        maps.put(BOOLEAN, booleanLiteral);

        //date
        FunctionCall call = new FunctionCall(QualifiedName.of("GETDATE"), false, Collections.emptyList());
        maps.put(DATE, call);
        maps.put(DATETIME, call);
        maps.put(TIMESTAMP, call);

        /**
         * for subClass override
         */
        reDefine(maps);
    }

    /**
     * ref define  maps
     *
     * @param maps
     */
    protected void reDefine(Map<IDataTypeName, BaseExpression> maps) {

    }

    public BaseExpression getSimpleData(BaseDataType baseDataType) {
        IDataTypeName typeName = baseDataType.getTypeName();
        DataTypeEnums dataTypeEnums = DataTypeEnums.getDataType(typeName.getValue());
        Dimension dimension = dataTypeEnums.getDimension();
        if (dimension == Dimension.MULTIPLE) {
            return getFunctionCall((GenericDataType)baseDataType, dataTypeEnums.getValue());
        } else {
            return basicExpression(baseDataType);
        }
    }

    private BaseExpression basicExpression(BaseDataType baseDataType) {
        IDataTypeName typeName = baseDataType.getTypeName();
        DataTypeEnums dataTypeEnums = DataTypeEnums.getDataType(typeName.getValue());
        BaseExpression baseExpression = maps.get(dataTypeEnums);
        if (baseExpression != null) {
            return baseExpression;
        }
        return new NullLiteral(null, null);
    }

    private FunctionCall getFunctionCall(GenericDataType baseDataType, String functionName) {
        List<BaseExpression> values = Lists.newArrayList();
        List<DataTypeParameter> arguments = baseDataType.getArguments();
        for (DataTypeParameter a : arguments) {
            if (a instanceof TypeParameter) {
                TypeParameter numericParameter = (TypeParameter)a;
                BaseExpression baseExpression = basicExpression(numericParameter.getType());
                values.add(baseExpression);
            }
        }
        return QueryUtil.functionCall(functionName, values.toArray(new BaseExpression[0]));
    }
}
