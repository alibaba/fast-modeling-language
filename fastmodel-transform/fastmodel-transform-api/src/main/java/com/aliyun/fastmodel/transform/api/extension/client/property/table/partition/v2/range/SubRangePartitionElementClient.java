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

import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.v2.SubPartitionElementClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * range sub partition element client
 *
 * @author panguanjing
 * @date 2024/2/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubRangePartitionElementClient extends SubPartitionElementClient {
    /**
     * 分区名
     */
    private String name;
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

    @Data
    @SuperBuilder
    public static class RangeExpression {
        private String dataType;
        private String expression;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    @SuperBuilder
    public static class RangeIntervalExpression extends RangeExpression {
        private String interval;
    }
}
