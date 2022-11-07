/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.conveter.dqc.util;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import org.apache.commons.lang3.StringUtils;

/**
 * 通用的一些方法类
 *
 * @author panguanjing
 * @date 2021/6/21
 */
public class FmlTableUtil {

    /**
     * 根据列的信息获取分区信息
     *
     * @param createTable 列的信息
     * @return {@see PartitionSpec}
     */
    public static List<PartitionSpec> getPartitionSpec(CreateTable createTable) {
        if (createTable == null || createTable.isPartitionEmpty()) {
            return null;
        }
        return createTable.getPartitionedBy().getColumnDefinitions().stream().map(
            x -> {
                List<Property> columnProperties = x.getColumnProperties();
                if (columnProperties == null || columnProperties.isEmpty()) {
                    return new PartitionSpec(x.getColName(), null);
                }
                Optional<Property> first = columnProperties.stream().filter(
                    p -> StringUtils.equalsIgnoreCase(p.getName(), ColumnPropertyDefaultKey.partition_spec.name())).findFirst();
                if (first.isPresent()) {
                    return new PartitionSpec(x.getColName(), first.get().getValueLiteral());
                }
                return new PartitionSpec(x.getColName(), null);
            }
        ).collect(Collectors.toList());
    }
}
