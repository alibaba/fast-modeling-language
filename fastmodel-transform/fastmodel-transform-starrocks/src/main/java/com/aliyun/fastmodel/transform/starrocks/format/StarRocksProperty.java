package com.aliyun.fastmodel.transform.starrocks.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * StarRocksProperty
 * $$是代表系统属性，不会进行输出
 *
 * @author panguanjing
 * @date 2023/9/14
 */
public enum StarRocksProperty implements PropertyKey {
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
     * index type
     */
    TABLE_INDEX_TYPE("index_type"),
    /**
     * index comment
     */
    TABLE_INDEX_COMMENT("index_comment"),

    /**
     * rollup
     */
    TABLE_ROLLUP("rollup"),

    /**
     * range partition
     */
    TABLE_RANGE_PARTITION("range_partition"),

    /**
     * table range partition raw
     */
    TABLE_PARTITION_RAW("range_partition_raw"),

    /**
     * char set
     */
    COLUMN_CHAR_SET("char_set"),

    /**
     * column key
     */
    COLUMN_KEY("column_key"),

    /**
     * column agg desc
     */
    COLUMN_AGG_DESC("column_agg_desc"),

    /**
     * REPLICATION_NUM
     */
    TABLE_REPLICATION_NUM("replication_num", true);

    private final String value;

    private final boolean supportPrint;

    StarRocksProperty(String value) {
        this(value, false);
    }

    StarRocksProperty(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static StarRocksProperty getByValue(String value) {
        StarRocksProperty[] starRocksProperties = StarRocksProperty.values();
        for (StarRocksProperty starRocksProperty : starRocksProperties) {
            if (StringUtils.equalsIgnoreCase(starRocksProperty.getValue(), value)) {
                return starRocksProperty;
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
