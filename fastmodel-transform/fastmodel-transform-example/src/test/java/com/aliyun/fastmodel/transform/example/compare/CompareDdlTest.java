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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.NodeCompareFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Compare DDL
 *
 * @author panguanjing
 * @date 2021/9/3
 */
public class CompareDdlTest {

    @Test
    public void testCompare() {
        List<ColumnDefinition> columnDefines = beforeColumnDefine();
        CreateTable before = getCreateTable(columnDefines);
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.DEFAULT_MYSQL);
        DialectNode beforeDialectNode = transformer.transform(before);
        String mysqlDDL
            = "create table dim_shop (c1 bigint comment 'abc', c2 varchar(128) comment 'c2') COMMENT 'comment'";
        List<BaseStatement> delegate = NodeCompareFactory.getInstance()
            .compare(DialectMeta.DEFAULT_MYSQL, beforeDialectNode.getNode(), mysqlDDL,
                CompareContext.builder().build());
        String print = print(delegate);
        assertEquals(print, "ALTER TABLE dim_shop DROP COLUMN a;\n"
            + "ALTER TABLE dim_shop ADD COLUMNS\n"
            + "(\n"
            + "   c2 VARCHAR(128) COMMENT 'c2'\n"
            + ")");
    }

    private String print(List<BaseStatement> delegate) {
        String collect = delegate.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
        System.out.println(collect);
        return collect;
    }

    private CreateTable getCreateTable(List<ColumnDefinition> columnDefines) {
        CreateTable before = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .comment(new Comment("comment"))
            .aliasedName(new AliasedName("alias"))
            .columns(columnDefines)
            .partition(new PartitionedBy(
                ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("a"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .category(ColumnCategory.CORRELATION)
                        .build()
                )
            ))
            .build();
        return before;
    }

    @Test
    public void testZenCoding() {
        List<ColumnDefinition> columnDefines = beforeColumnDefine();
        String zencoding = "user_id\nuser_name";
        CreateTable before = getCreateTable(columnDefines);
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getByName(DialectName.ZEN));
        DialectNode beforeDialectNode = transformer.transform(before);
        List<BaseStatement> diff = NodeCompareFactory.getInstance().compare(DialectMeta.getByName(DialectName.ZEN),
            beforeDialectNode.getNode(),
            zencoding, CompareContext.builder().qualifiedName(QualifiedName.of("dim_shop")).build());
        assertEquals(print(diff), "ALTER TABLE dim_shop DROP COLUMN c1;\n"
            + "ALTER TABLE dim_shop ADD COLUMNS\n"
            + "(\n"
            + "   user_id   STRING COMMENT 'user_id',\n"
            + "   user_name STRING COMMENT 'user_name'\n"
            + ")");
    }

    private List<ColumnDefinition> beforeColumnDefine() {
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition build = ColumnDefinition.builder().colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .comment(new Comment("abc"))
            .build();
        list.add(build);
        return list;
    }
}
