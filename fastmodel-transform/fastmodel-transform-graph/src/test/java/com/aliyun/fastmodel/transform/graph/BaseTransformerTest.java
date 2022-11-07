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

package com.aliyun.fastmodel.transform.graph;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;

/**
 * provider the common data
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public class BaseTransformerTest {

    public CompositeStatement initBaseStatement() {
        List<ColumnDefinition> columns = initColumns();
        List<BaseConstraint> constraints = initConstraints();
        BaseStatement source = CreateTable
            .builder()
            .tableName(QualifiedName.of("table"))
            .comment(new Comment("comment"))
            .detailType(TableDetailType.NORMAL_DIM)
            .columns(columns)
            .constraints(constraints)
            .build();

        BaseStatement target = CreateTable
            .builder()
            .tableName(QualifiedName.of("table1"))
            .detailType(TableDetailType.NORMAL_DIM)
            .comment(new Comment("comment1"))
            .columns(columns)
            .build();

        RefObject left = new RefObject(QualifiedName.of("table"), null, new Comment("comment"));
        RefObject right = new RefObject(QualifiedName.of("table1"), null, new Comment("comment"));

        RefRelation refRelation = new RefRelation(QualifiedName.of("relName"), left, right,
            RefDirection.LEFT_DIRECTION_RIGHT);

        CompositeStatement compositeStatement = new CompositeStatement(Lists.newArrayList(source, refRelation, target));
        return compositeStatement;
    }

    private List<BaseConstraint> initConstraints() {
        DimConstraint dimConstraint = new DimConstraint(
            new Identifier("c1"),
            ImmutableList.of(new Identifier("c1")),
            QualifiedName.of("t2"),
            Lists.newArrayList(new Identifier("cc"))
        );
        return ImmutableList.of(dimConstraint);
    }

    private List<ColumnDefinition> initColumns() {
        ColumnDefinition columnDefinition = ColumnDefinition
            .builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE))
            .aliasedName(new AliasedName("a1"))
            .build();
        return ImmutableList.of(columnDefinition);
    }

    public String expectFromFile(String fileName) throws IOException {
        return IOUtils.resourceToString("/test/" + fileName, StandardCharsets.UTF_8);
    }
}
