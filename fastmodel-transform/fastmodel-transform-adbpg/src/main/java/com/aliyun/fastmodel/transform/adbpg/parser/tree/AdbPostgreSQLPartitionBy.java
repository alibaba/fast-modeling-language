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

package com.aliyun.fastmodel.transform.adbpg.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.adbpg.parser.visitor.AdbPostgreSQLVisitor;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.PartitionByType;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import lombok.Getter;

/**
 * 定义adb pg的分区类型，参考文档：
 * <a href="https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html#examples">...</a>
 *
 * @author panguanjing
 * @date 2024/10/17
 */
@Getter
public class AdbPostgreSQLPartitionBy extends PartitionedBy {

    private final PartitionByType partitionByType;

    private final List<BaseSubPartition> subPartitions;

    private final List<BasePartitionElement> partitionElements;

    public AdbPostgreSQLPartitionBy(List<ColumnDefinition> columnDefinitions, PartitionByType partitionByType, List<BaseSubPartition> subPartitions,
        List<BasePartitionElement> partitionElements) {
        super(columnDefinitions);
        this.partitionByType = partitionByType;
        this.subPartitions = subPartitions;
        this.partitionElements = partitionElements;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        AdbPostgreSQLVisitor<R, C> adbPostgreSQLVisitor = (AdbPostgreSQLVisitor<R, C>)visitor;
        return adbPostgreSQLVisitor.visitAdbPostgreSQLPartitionBy(this, context);
    }
}
