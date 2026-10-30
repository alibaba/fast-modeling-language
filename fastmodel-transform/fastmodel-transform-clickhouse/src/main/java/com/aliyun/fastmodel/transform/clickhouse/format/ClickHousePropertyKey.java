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

package com.aliyun.fastmodel.transform.clickhouse.format;

import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * ClickHouse property key
 * <p>
 * 表级的引擎子句以属性的方式在 Table DTO 与 FML AST 之间传递，属性名与 ClickHouse 的建表子句一一对应。
 * 这些属性由 {@link ClickHouseOutVisitor} 显式输出，不走通用的 PROPERTIES 打印。
 *
 * @author panguanjing
 */
public enum ClickHousePropertyKey implements PropertyKey {

    /**
     * 表引擎，如 MergeTree、ReplacingMergeTree，建表必填
     */
    TABLE_ENGINE("engine"),

    /**
     * 分区键表达式，如 toYYYYMM(create_time)
     */
    TABLE_PARTITION_BY("partition_by"),

    /**
     * 建表时下发的集群名,对应 ON CLUSTER 子句
     */
    TABLE_CLUSTER("cluster"),

    /**
     * 主键表达式，如 (id, name)
     */
    TABLE_PRIMARY_KEY("primary_key"),

    /**
     * 排序键表达式，如 (id, create_time)
     */
    TABLE_ORDER_BY("order_by"),

    /**
     * 采样键表达式
     */
    TABLE_SAMPLE_BY("sample_by"),

    /**
     * 数据存活时间表达式，如 create_time + INTERVAL 30 DAY
     */
    TABLE_TTL("ttl"),

    /**
     * 表级参数，如 index_granularity = 8192
     */
    TABLE_SETTINGS("settings");

    private final String value;

    private final boolean supportPrint;

    ClickHousePropertyKey(String value) {
        this(value, false);
    }

    ClickHousePropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static PropertyKey getByValue(String value) {
        for (ClickHousePropertyKey clickHousePropertyKey : values()) {
            if (StringUtils.equalsIgnoreCase(clickHousePropertyKey.getValue(), value)) {
                return clickHousePropertyKey;
            }
        }
        for (ExtensionPropertyKey extensionPropertyKey : ExtensionPropertyKey.values()) {
            if (StringUtils.equalsIgnoreCase(extensionPropertyKey.getValue(), value)) {
                return extensionPropertyKey;
            }
        }
        return null;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isSupportPrint() {
        return supportPrint;
    }
}
