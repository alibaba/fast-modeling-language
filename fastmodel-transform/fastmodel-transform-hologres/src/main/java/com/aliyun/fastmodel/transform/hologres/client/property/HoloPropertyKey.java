package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/1/18
 */
public enum HoloPropertyKey implements PropertyKey {

    /**
     * bitmap_columns
     */
    BITMAP_COLUMN("bitmap_columns", true),

    CLUSTERING_KEY("clustering_key", true),

    DICTIONARY_ENCODING_COLUMN("dictionary_encoding_columns", true),

    DISTRIBUTION_KEY("distribution_key", true),

    EVENT_TIME_COLUMN("event_time_column", true),

    TABLE_GROUP("table_group", true),

    ORIENTATION("orientation", true),

    ENABLE_BINLOG("binlog.level", true),

    BINLOG_TTL("binlog.ttl", true),

    TIME_TO_LIVE_IN_SECONDS("time_to_live_in_seconds", true),

    SEGMENT_KEY("segment_key", true),

    /**
     * 是否外表
     */
    FOREIGN("foreign", false),

    /**
     * 连接外部数据源的服务器
     */
    SERVER_NAME("server_name", false)
    ;

    private String value;

    private boolean supportPrint;

    HoloPropertyKey(String value) {
        this(value, false);
    }

    HoloPropertyKey(String value, boolean supportPrint) {
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

    public static HoloPropertyKey getByValue(String value) {
        HoloPropertyKey[] holoPropertyKeys = HoloPropertyKey.values();
        for (HoloPropertyKey m : holoPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
        }
        return null;
    }

}
