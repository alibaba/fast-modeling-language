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

package com.aliyun.fastmodel.transform.api.extension.client.property;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/1/22
 */
public enum ExtensionPropertyKey implements PropertyKey {

    /**
     * life cycle
     */
    LIFE_CYCLE("life_cycle"),

    /**
     * engine
     */
    TABLE_ENGINE("engine"),

    /**
     * distribute hash
     */
    TABLE_DISTRIBUTED_HASH("distributed_hash"),

    /**
     * distribute buckets
     */
    TABLE_DISTRIBUTED_BUCKETS("distributed_buckets"),

    /**
     * column key
     */
    COLUMN_KEY("column_key"),

    /**
     * 自增列信息
     */
    COLUMN_AUTO_INCREMENT("column_auto_increment"),

    /**
     * column agg desc
     */
    COLUMN_AGG_DESC("column_agg_desc"),

    /**
     * colum  check
     */
    COLUMN_CHECK("column_check"),

    /**
     * index type
     */
    TABLE_INDEX_TYPE("index_type"),
    /**
     * index comment
     */
    TABLE_INDEX_COMMENT("index_comment"),

    /**
     * range partition
     */
    TABLE_RANGE_PARTITION("range_partition"),

    /**
     * table range partition raw
     */
    TABLE_PARTITION_RAW("partition_raw"),

    /**
     * Table partition
     */
    TABLE_PARTITION("partition"),

    /**
     * list partition
     */
    TABLE_LIST_PARTITION("list_partition"),

    /**
     * expression partition
     */
    TABLE_EXPRESSION_PARTITION("expression_partition"),

    /**
     * REPLICATION_NUM
     */
    TABLE_REPLICATION_NUM("replication_num", true),

    /**
     * 保留最近多少数量的分区
     */
    PARTITION_LIVE_NUMBER("partition_live_number", true),

    /**
     * cluster key
     */
    CLUSTERING_KEY("clustering_key", true),

    /**
     * external
     */
    EXTERNAL("external");

    private final String value;

    private final boolean supportPrint;

    ExtensionPropertyKey(String value) {
        this(value, false);
    }

    ExtensionPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isSupportPrint() {
        return supportPrint;
    }

    public static ExtensionPropertyKey getByValue(String value) {
        ExtensionPropertyKey[] extensionPropertyKeys = ExtensionPropertyKey.values();
        for (ExtensionPropertyKey e : extensionPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(e.getValue(), value)) {
                return e;
            }
        }
        return null;
    }
}
