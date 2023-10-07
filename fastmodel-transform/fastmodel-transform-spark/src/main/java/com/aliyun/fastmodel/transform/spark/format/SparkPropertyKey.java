package com.aliyun.fastmodel.transform.spark.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * spark sql property key
 *
 * @author panguanjing
 * @date 2023/2/16
 */
public enum SparkPropertyKey implements PropertyKey {
    /**
     * clustered by
     */
    CLUSTERED_BY("clustered_by"),

    /**
     * sorted by
     */
    SORTED_BY("sorted_by"),

    /**
     * number buckets
     */
    NUMBER_BUCKETS("num_buckets"),

    /**
     * using
     */
    USING("using");

    /**
     * value
     */
    private String value;

    /**
     * support print
     */
    private boolean supportPrint;

    SparkPropertyKey(String value) {
        this(value, false);
    }

    SparkPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static SparkPropertyKey getByValue(String value) {
        SparkPropertyKey[] sparkPropertyKeys = SparkPropertyKey.values();
        for (SparkPropertyKey sparkPropertyKey : sparkPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(sparkPropertyKey.getValue(), value)) {
                return sparkPropertyKey;
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
