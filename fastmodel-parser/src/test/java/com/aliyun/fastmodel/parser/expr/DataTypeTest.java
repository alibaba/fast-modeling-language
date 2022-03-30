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

package com.aliyun.fastmodel.parser.expr;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/11/17
 */
public class DataTypeTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testVarcharTypeParameter() {
        String expr = "varchar(2)";
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage(expr));
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        assertEquals(genericDataType.getTypeName(), DataTypeEnums.VARCHAR);
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        DataTypeParameter dataTypeParameter = arguments.get(0);
        NumericParameter numericParameter = (NumericParameter)dataTypeParameter;
        assertEquals(numericParameter.getValue(), "2");
    }

    @Test
    public void testCharTypeParameter() {
        String expr = "char(2)";
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage(expr));
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        NumericParameter numericParameter = (NumericParameter)arguments.get(0);
        assertEquals(numericParameter.getValue(), "2");
    }

    @Test
    public void testDecimalTypeParameter() {
        String expr = "decimal(2, 10)";
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage(expr));
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        List<DataTypeParameter> dataTypeParameters = Collections.unmodifiableList(genericDataType.getArguments());
        NumericParameter first = (NumericParameter)dataTypeParameters.get(0);
        NumericParameter second = (NumericParameter)dataTypeParameters.get(1);
        assertEquals(first.getValue(), "2");
        assertEquals(second.getValue(), "10");
    }

    @Test
    public void testDouble() {
        String expr = "DOUBLE";
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage(expr));
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        assertEquals(genericDataType.getTypeName(), DataTypeEnums.DOUBLE);
    }

    @Test
    public void testCustom() {
        String expr = "custom('varchar2')(1)";
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage(expr));
        GenericDataType genericDataType = (GenericDataType)baseDataType;
        DataTypeEnums typeName = genericDataType.getTypeName();
        assertEquals(genericDataType.toString(), "CUSTOM('varchar2')(1)");
    }
}
