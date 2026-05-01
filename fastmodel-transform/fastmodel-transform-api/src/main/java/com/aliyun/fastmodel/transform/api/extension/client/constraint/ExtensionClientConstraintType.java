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

package com.aliyun.fastmodel.transform.api.extension.client.constraint;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import lombok.Getter;

/**
 * StarRocksConstraintType
 *
 * @author panguanjing
 * @date 2023/12/13
 */
@Getter
public enum ExtensionClientConstraintType implements ConstraintType {

    /**
     * aggregate key
     */
    AGGREGATE_KEY("aggregate_key"),

    /**
     * duplicate key
     */
    DUPLICATE_KEY("duplicate_key"),
    /**
     * unique key
     */
    UNIQUE_KEY(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.UNIQUE.getCode()),
    /**
     * primary key
     */
    PRIMARY_KEY(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.PRIMARY_KEY.getCode()),

    /**
     * order by
     */
    ORDER_BY("orderBy"),

    /**
     * distribute key
     */
    DISTRIBUTE("distribute"),

    /**
     * watermark (flink)
     */
    WATERMARK("watermark"),

    /**
     * cluster by
     */
    CLUSTER_BY("clusterBy"),

    /**
     * clustered key
     */
    CLUSTERED_KEY("clusteredKey"),

    /**
     * 向量索引
     */
    ANN_INDEX("annIndex"),

    /**
     * 外键
     */
    FOREIGN_KEY("foreignKey");

    private final String code;

    ExtensionClientConstraintType(String code) {this.code = code;}

    @Override
    public String getCode() {
        return code;
    }

    public static ExtensionClientConstraintType getByValue(String value) {
        for (ExtensionClientConstraintType starRocksConstraintType : ExtensionClientConstraintType.values()) {
            if (starRocksConstraintType.code.equalsIgnoreCase(value)) {
                return starRocksConstraintType;
            }
        }
        return null;
    }
}
