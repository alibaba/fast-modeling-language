package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/19
 */
public enum MaxComputePropertyKey implements PropertyKey {
    /**
     * life cycle
     */
    LIFE_CYCLE("life_cycle"),

    /**
     * transactional
     */
    TRANSACTIONAL("transactional", true),

    /**
     * append mode
     */
    APPEND_MODE("append_mode", true),

    /**
     * write.bucket.num
     */
    WRITE_BUCKET_NUM("write.bucket.num", true),

    /**
     * acid.data.retain.hours
     */
    ACID_DATA_RETAIN_HOURS("acid.data.retain.hours", true),

    /**
     * acid.incremental.query.out.of.time.range.enabled
     */
    ACID_INCREMENTAL_QUERY_OUT_OF_TIME_RANGE_ENABLED("acid.incremental.query.out.of.time.range.enabled", true),

    /**
     * table format version
     */
    TABLE_FORMAT_VERSION("table.format.version", true),

    /**
     * external
     */
    EXTERNAL("external", false),

    /**
     * location
     * effect when external
     */
    LOCATION("location", false),

    /**
     * stored by
     * effect when external
     */
    STORED_BY("stored_by", false),

    /**
     * row format serde
     * effect when external
     */
    ROW_FORMAT_SERDE("row_format_serde", false),

    /**
     * serde props
     * effect when external
     */
    SERDE_PROPS("serde_props", false),

    /**
     * stored as
     * effect when external
     */
    STORED_AS("stored_as", false),

    /**
     * using
     * effect when external
     */
    USING("using", false),

    /**
     * stored as
     * effect when external
     */
    INPUT_FORMAT("input_format", false),

    /**
     * stored as
     * effect when external
     */
    OUTPUT_FORMAT("output_format", false),

    /**
     * clustered by
     */
    CLUSTERED_BY("clustered_by", false),

    /**
     * number of buckets
     */
    NUMBER_OF_BUCKETS("number_of_buckets", false),

    /**
     * auto partitioned by
     */
    AUTO_PARTITIONED_BY("auto_partitioned_by", false),

    /**
     * ingestion_time_partition
     */
    INGESTION_TIME_PARTITION("ingestion_time_partition", true),
    ;

    private final String value;

    private final boolean supportPrint;

    MaxComputePropertyKey(String value) {
        this(value, false);
    }

    MaxComputePropertyKey(String value, boolean supportPrint) {
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

    public static MaxComputePropertyKey getByValue(String value) {
        MaxComputePropertyKey[] maxComputePropertyKeys = MaxComputePropertyKey.values();
        for (MaxComputePropertyKey m : maxComputePropertyKeys) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
        }
        return null;
    }

}
