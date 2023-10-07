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

package com.aliyun.fastmodel.transform.plantuml;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateIndicator;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.impl.IndicatorFragmentParser;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 指标片段处理
 *
 * @author panguanjing
 * @date 2020/9/21
 */
public class IndicatorFragmentParserTest {

    IndicatorFragmentParser indicatorFragmentParser = new IndicatorFragmentParser();

    @Test
    public void testParse() {
        List<BaseExpression> field1 = ImmutableList.of(new TableOrColumn(QualifiedName.of("field1")));
        FunctionCall baseExpression = new FunctionCall(QualifiedName.of("sum"), false, field1);
        CreateIndicator createIndicatorStatement = new CreateIndicator(
            CreateElement.builder().qualifiedName(QualifiedName.of("indicator")).build(),
            DataTypeUtil.simpleType(DataTypeEnums.STRING), baseExpression, IndicatorType.ATOMIC
        );
        Fragment parse = indicatorFragmentParser.parse(createIndicatorStatement);
        String content = parse.content();
        assertNotNull(content);
        assertTrue(content.contains("indicator"));
    }

    @Test
    public void testVisualException() {
        CreateIndicator createIndicatorStatement = new CreateIndicator(
            CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build(),
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT), null, null);
        try {
            indicatorFragmentParser.parse(createIndicatorStatement);
        } catch (VisualParseException e) {
            assertNotNull(e);
        }
    }
}
