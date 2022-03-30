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

package com.aliyun.fastmodel.converter.util;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;

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
     * @param columnList 列的信息
     * @return {@see PartitionSpec}
     */
    public static List<PartitionSpec> getPartitionSpec(CreateTable columnList) {
        if (columnList == null || columnList.isPartitionEmpty()) {
            return null;
        }
        return columnList.getPartitionedBy().getColumnDefinitions().stream().map(
            x -> {
                return new PartitionSpec(x.getColName(), null);
            }
        ).collect(Collectors.toList());
    }
}
