/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbmysql.client.converter;

import java.util.List;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbmysql.parser.AdbMysqlLanguageParser;
import com.aliyun.fastmodel.transform.adbmysql.parser.tree.AdbMysqlDataTypeName;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/3/24
 */
public class AdbMysqlClientConverterTest {

    AdbMysqlClientConverter adbMysqlClientConverter = new AdbMysqlClientConverter();

    @Test
    public void getLanguageParser() {
        LanguageParser languageParser = adbMysqlClientConverter.getLanguageParser();
        assertEquals(AdbMysqlLanguageParser.class, languageParser.getClass());
    }

    @Test
    public void getPropertyConverter() {
        PropertyConverter propertyConverter = adbMysqlClientConverter.getPropertyConverter();
        assertEquals(AdbMysqlPropertyConverter.class, propertyConverter.getClass());
    }

    @Test
    public void getDataTypeName() {
        IDataTypeName array = adbMysqlClientConverter.getDataTypeName("array<string>");
        assertEquals(AdbMysqlDataTypeName.ARRAY, array);
    }

    @Test
    public void getRaw() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType(
            AdbMysqlDataTypeName.JSON, null
        )).build());
        Node node = CreateTable.builder()
            .tableName(QualifiedName.of("t1"))
            .columns(columns)
            .build();
        String raw = adbMysqlClientConverter.getRaw(node);
        assertEquals("CREATE TABLE t1\n"
            + "(\n"
            + "   c1 JSON\n"
            + ")", raw);
    }
}