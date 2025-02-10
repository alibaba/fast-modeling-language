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

package com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.list;

import java.util.List;

import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.PartitionElementClient;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ListPartitionElementClient
 *
 * @author panguanjing
 * @date 2024/2/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ListPartitionElementClient extends PartitionElementClient {
    private String qualifiedName;

    private Boolean defaultExpr;

    private List<String> expressionList;

    private Long num;

    private String engine;

    private List<SubPartitionElementClient> subPartitionElementClients;
}
