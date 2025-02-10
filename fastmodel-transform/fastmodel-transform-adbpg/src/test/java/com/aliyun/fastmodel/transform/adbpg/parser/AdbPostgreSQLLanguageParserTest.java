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

package com.aliyun.fastmodel.transform.adbpg.parser;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/13
 */
public class AdbPostgreSQLLanguageParserTest {

    AdbPostgreSQLLanguageParser languageParser = new AdbPostgreSQLLanguageParser();

    @Test
    public void parseNode() {
        CreateTable o = languageParser.parseNode(
            "CREATE TABLE sales (txn_id int, qty int, date date) \n" + "WITH (appendoptimized=true, compresslevel=5) \n"
                + "DISTRIBUTED BY (txn_id);");
        assertNotNull(o);
        List<ColumnDefinition> columnDefines = o.getColumnDefines();
        assertEquals(3, columnDefines.size());
        List<Property> properties = o.getProperties();
        assertEquals(2, properties.size());
        List<BaseConstraint> constraintStatements = o.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        BaseConstraint baseConstraint = constraintStatements.get(0);
        DistributeNonKeyConstraint distributeNonKeyConstraint = (DistributeNonKeyConstraint)baseConstraint;
        assertEquals(1, distributeNonKeyConstraint.getColumns().size());
    }

    @Test
    @SneakyThrows
    public void testParseNodeWith() {
        String t = IOUtils.resourceToString("/adbpostgresql/default.txt", Charset.defaultCharset());
        CreateTable createTable = languageParser.parseNode(t);
        assertNotNull(createTable);
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        BaseExpression defaultValue = columnDefinition.getDefaultValue();
        assertNotNull(defaultValue);

        ColumnDefinition columnDefinition1 = columnDefines.get(1);
        List<Property> columnProperties = columnDefinition1.getColumnProperties();
        Property property = columnProperties.stream().filter(c -> {
            return StringUtils.equalsIgnoreCase(c.getName(), ExtensionPropertyKey.COLUMN_CHECK.getValue());
        }).findFirst().get();
        assertEquals("name <> ''", property.getValue());
    }

    @Test
    @SneakyThrows
    public void testPartitionBy() {
        String t = IOUtils.resourceToString("/adbpostgresql/partition_by.txt", Charset.defaultCharset());
        CreateTable createTable = languageParser.parseNode(t);
        assertNotNull(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertNotNull(partitionedBy);
    }

    @Test
    @SneakyThrows
    public void testSubPartitionBy() {
        String t = IOUtils.resourceToString("/adbpostgresql/sub_partition_by.txt", Charset.defaultCharset());
        CreateTable createTable = languageParser.parseNode(t);
        assertNotNull(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertNotNull(partitionedBy);
    }

    @SneakyThrows
    @Test
    public void testParseFilms() {
        String t = IOUtils.resourceToString("/adbpostgresql/films.txt", Charset.defaultCharset());
        CreateTable createTable = languageParser.parseNode(t);
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(6, columnDefines.size());
    }

    @Test
    @SneakyThrows
    public void testSubPartitionWithTemplate() {
        String t = IOUtils.resourceToString("/adbpostgresql/sub_partition_with_template.txt", Charset.defaultCharset());
        CreateTable createTable = languageParser.parseNode(t);
        assertNotNull(createTable);
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertNotNull(partitionedBy);
    }
}