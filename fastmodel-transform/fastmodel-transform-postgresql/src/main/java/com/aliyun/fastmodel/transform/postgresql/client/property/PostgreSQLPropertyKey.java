package com.aliyun.fastmodel.transform.postgresql.client.property;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * postgresql property key
 *
 * @author panguanjing
 */
public enum PostgreSQLPropertyKey implements PropertyKey {

    /**
     * 事件时间列
     */
    EVENT_TIME_COLUMN("event_time_column", true),

    /**
     * 是否启用binlog
     */
    ENABLE_BINLOG("binlog.level", true),

    /**
     * binlog TTL
     */
    BINLOG_TTL("binlog.ttl", true),

    /**
     * 有效期
     */
    TIME_TO_LIVE_IN_SECONDS("time_to_live_in_seconds", true),

    /**
     * segment key
     */
    SEGMENT_KEY("segment_key", true),

    /**
     * 是否外表
     */
    FOREIGN("foreign", false),

    /**
     * 连接外部数据源的服务器
     */
    SERVER_NAME("server_name", false),

    /**
     * 是否动态表
     */
    DYNAMIC("dynamic", false);

    /**
     * 具体的值
     */
    private final String value;

    /**
     * 是否前缀
     */
    @Getter
    private final boolean prefix;

    /**
     * 是否通过property打印输出
     */
    private final boolean supportPrint;

    PostgreSQLPropertyKey(String value) {
        this(value, false);
    }

    PostgreSQLPropertyKey(String value, boolean supportPrint) {
        this(value, supportPrint, false);
    }

    PostgreSQLPropertyKey(String value, boolean supportPrint, boolean prefix) {
        this.value = value;
        this.supportPrint = supportPrint;
        this.prefix = prefix;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isSupportPrint() {
        return supportPrint;
    }

    public static PostgreSQLPropertyKey getByValue(String value) {
        PostgreSQLPropertyKey[] holoPropertyKeys = PostgreSQLPropertyKey.values();
        for (PostgreSQLPropertyKey m : holoPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
            if (m.isPrefix() && value.startsWith(m.getValue())) {
                return m;
            }
        }
        return null;
    }

}
