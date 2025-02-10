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

package com.aliyun.fastmodel.transform.api.extension.tree.partition;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * partition by type
 *
 * @author panguanjing
 * @date 2024/10/17
 */
@Getter
public enum PartitionByType {

    /**
     * list
     */
    LIST("LIST"),

    /**
     * range
     */
    RANGE("RANGE"),

    /**
     * Hash
     */
    HASH("HASH"),

    /**
     * expression
     */
    EXPRESSION("EXPRESSION");

    private final String value;

    PartitionByType(String value) {
        this.value = value;
    }

    public static PartitionByType getByValue(String value) {
        PartitionByType[] partitionByTypes = PartitionByType.values();
        for (PartitionByType partitionByType : partitionByTypes) {
            if (StringUtils.equalsIgnoreCase(partitionByType.value, value)) {
                return partitionByType;
            }
        }
        return null;
    }

}
