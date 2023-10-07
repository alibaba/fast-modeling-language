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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.impl.TableFragmentParser;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/18
 */
public class TableFragmentParserTest {
    TableFragmentParser tableFragmentParser;

    @Before
    public void setUp() {
        tableFragmentParser = new TableFragmentParser();
    }

    @Test
    public void testParse() throws Exception {
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).build();

        list.add(columnDefine);
        PrimaryConstraint primaryConstraintStatement = new PrimaryConstraint(
            new Identifier("c1"), ImmutableList.of(new Identifier("c1")));
        ArrayList<BaseConstraint> constraintStatements = new ArrayList<>();
        constraintStatements.add(primaryConstraintStatement);
        CreateTable createTableStatement = CreateFactTable.builder().tableName(QualifiedName.of("identifier"))
            .columns(list).constraints(constraintStatements).build();
        Fragment result = tableFragmentParser.parse(createTableStatement);
        assertEquals(result.content(), "Table(identifier) {\n"
            + "Column(PK(\"c1\"), \"BIGINT\", 0,\"\", \"\", \"\")\n"
            + "}\n");
    }

    @Test
    public void testParseWithDim() {
        List<ColumnDefinition> list = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );

        List<BaseConstraint> constraintStatements = ImmutableList.of(
            new DimConstraint(
                new Identifier("c1"),
                QualifiedName.of("r1")
            )
        );
        CreateTable createTableStatement = CreateFactTable.builder()
            .tableName(QualifiedName.of("identifier"))
            .columns(list)
            .constraints(constraintStatements).build();

        Fragment result = tableFragmentParser.parse(createTableStatement);
        assertEquals(result.content(), "Table(identifier) {\n"
            + "Column(\"c1\", \"BIGINT\", 0,\"\", \"\", \"\")\n"
            + "}\n"
            + "identifier --> r1\n");
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev
// .com/forum#!/testme