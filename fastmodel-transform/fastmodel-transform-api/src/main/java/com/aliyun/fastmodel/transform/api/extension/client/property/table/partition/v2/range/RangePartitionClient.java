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
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangePartitionClient {

    private List<String> columns;

    private Long partitionCount;

    private String baseExpression;

    private List<PartitionElementClient> partitionElementClients;

    private List<SubPartitionClient> subPartitionClients;
}
