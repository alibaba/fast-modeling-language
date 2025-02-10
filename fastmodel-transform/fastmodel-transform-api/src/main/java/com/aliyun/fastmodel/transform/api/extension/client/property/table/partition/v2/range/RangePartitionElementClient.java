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

package com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range;

import java.util.List;

import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.PartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionElementClient.RangeExpression;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.range.SubRangePartitionElementClient.RangeIntervalExpression;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RangePartitionElementClient
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangePartitionElementClient extends PartitionElementClient {

    /**
     * 分区名
     */
    private String qualifiedName;

    /**
     * 是否默认
     */
    private Boolean defaultExpr;

    /**
     * 开始表达式
     */
    private RangeExpression start;

    /**
     * 结束表达式
     */
    private RangeExpression end;
    /**
     * every表达式
     */
    private RangeIntervalExpression every;

    /**
     * sub partition element client
     */
    private List<SubPartitionElementClient> subPartitionElementClients;
}
