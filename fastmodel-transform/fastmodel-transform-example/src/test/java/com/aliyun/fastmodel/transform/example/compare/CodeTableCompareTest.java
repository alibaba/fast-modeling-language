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

package com.aliyun.fastmodel.transform.example.compare;

import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.parser.NodeParser;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/31
 */
public class CodeTableCompareTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testCodeTableWithDelimited() {
        String fml = "CREATE CODE TABLE IF NOT EXISTS jiawa_test_01 ALIAS 'jiawa_test_01' \n"
            + "(\n"
            + "   `code` ALIAS 'code' STRING COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING COMMENT 'name',\n"
            + "   extendName ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + " COMMENT 'jiawa_test_01'";
        BaseStatement statement = nodeParser.parseStatement(fml);
        assertEquals(statement.toString(), "CREATE CODE TABLE IF NOT EXISTS jiawa_test_01 ALIAS 'jiawa_test_01' \n"
            + "(\n"
            + "   `code`      ALIAS 'code' STRING NOT NULL COMMENT 'code',\n"
            + "   name        ALIAS 'name' STRING COMMENT 'name',\n"
            + "   extendname  ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + "COMMENT 'jiawa_test_01'");
    }

    @Test
    public void testCompare() {
        String fml = "CREATE CODE TABLE IF NOT EXISTS jiawa_test_01 ALIAS 'jiawa_test_01' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING COMMENT 'name',\n"
            + "   extendName ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + " COMMENT 'jiawa_test_01'";
        BaseStatement baseStatement = nodeParser.parseStatement(fml);

        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("code"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("code"))
                .aliasedName(new AliasedName("code"))
                .notNull(true)
                .build(),
            ColumnDefinition.builder().colName(new Identifier("name"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("name"))
                .aliasedName(new AliasedName("name"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("extendName"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("extendName"))
                .aliasedName(new AliasedName("extendName"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("description"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("description"))
                .aliasedName(new AliasedName("description"))
                .build()

        );

        List<BaseConstraint> constraints = ImmutableList.of(
            new PrimaryConstraint(
                new Identifier("pk_code"),
                ImmutableList.of(new Identifier("code"))
            )
        );
        CreateCodeTable createCodeTable = CreateCodeTable.builder()
            .ifNotExist(true)
            .tableName(QualifiedName.of("jiawa_test_01"))
            .aliasedName(new AliasedName("jiawa_test_01"))
            .columns(columns)
            .constraints(constraints).comment(new Comment("jiawa_test_01"))
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(baseStatement, createCodeTable,
            CompareStrategy.INCREMENTAL);
        assertEquals(0, compare.size());
    }

    @Test
    public void testCompareBugfix() {
        String c = "CREATE CODE TABLE IF NOT EXISTS jiawa_test_01 ALIAS 'jiawa_test_01' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING NOT NULL COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING NOT NULL COMMENT 'name',\n"
            + "   extendName ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING NOT NULL COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + " COMMENT 'jiawa_test_01'";
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("code"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("code"))
                .aliasedName(new AliasedName("code"))
                .notNull(true)
                .build(),
            ColumnDefinition.builder().colName(new Identifier("name"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .aliasedName(new AliasedName("name"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("extendName"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("extendName"))
                .aliasedName(new AliasedName("extendName"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("description"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("description"))
                .aliasedName(new AliasedName("description"))
                .build()

        );

        List<BaseConstraint> constraints = ImmutableList.of(
            new PrimaryConstraint(
                new Identifier("pk_code"),
                ImmutableList.of(new Identifier("code"))
            )
        );
    }
}
