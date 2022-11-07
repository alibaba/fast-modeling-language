/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.dqc.util;

import java.util.List;

import com.aliyun.fastmodel.conveter.dqc.util.FmlTableUtil;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/8
 */
public class FmlTableUtilTest {

    @Test
    public void getPartitionSpec() {
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .build();
        List<PartitionSpec> partitionSpec = FmlTableUtil.getPartitionSpec(table);
        assertNull(partitionSpec);
    }

    @Test
    public void testPartitionSpecWithKey() {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .properties(Lists.newArrayList(new Property(ColumnPropertyDefaultKey.partition_spec.name(), "abc")))
            .build();
        PartitionedBy partitionedBy = new PartitionedBy(
            Lists.newArrayList(c1)
        );
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(partitionedBy)
            .build();
        List<PartitionSpec> partitionSpec = FmlTableUtil.getPartitionSpec(table);
        assertEquals(1, partitionSpec.size());
        PartitionSpec partitionSpec1 = partitionSpec.get(0);
        assertEquals(((StringLiteral)partitionSpec1.getBaseLiteral()).getValue(), "abc");
    }
}