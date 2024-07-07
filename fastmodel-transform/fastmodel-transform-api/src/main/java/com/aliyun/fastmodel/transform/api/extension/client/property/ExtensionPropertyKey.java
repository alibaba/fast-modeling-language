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
    PARTITION_LIVE_NUMBER("partition_live_number", true);

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
