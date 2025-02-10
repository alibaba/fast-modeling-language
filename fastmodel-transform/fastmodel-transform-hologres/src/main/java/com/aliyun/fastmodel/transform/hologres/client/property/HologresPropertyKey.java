package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/1/18
 */
public enum HologresPropertyKey implements PropertyKey {

    /**
     * bitmap_columns
     */
    BITMAP_COLUMN("bitmap_columns", true),


    /**
     * 字典编码列
     */
    DICTIONARY_ENCODING_COLUMN("dictionary_encoding_columns", true),

    /**
     * 分布式
     */
    DISTRIBUTION_KEY("distribution_key", true),

    /**
     * cluster key
     */
    CLUSTERING_KEY(ExtensionPropertyKey.CLUSTERING_KEY.getValue(), true),

    /**
     * 事件时间列
     */
    EVENT_TIME_COLUMN("event_time_column", true),

    /**
     * 表组
     */
    TABLE_GROUP("table_group", true),

    /**
     * 方向
     */
    ORIENTATION("orientation", true),

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

    ///////////dynamic table key start
    /**
     * refresh mode
     */
    REFRESH_MODE("refresh_mode", true),

    /**
     * auto_refresh_enable
     */
    AUTO_REFRESH_ENABLE("auto_refresh_enable", true),

    /**
     * incremental_auto_refresh_schd_start_time
     */
    INCREMENTAL_AUTO_REFRESH_SCHD_START_TIME("incremental_auto_refresh_schd_start_time", true),

    /**
     * incremental_auto_refresh_interval
     */
    INCREMENTAL_AUTO_REFRESH_INTERVAL("incremental_auto_refresh_interval", true),

    /**
     * incremental_guc_hg_incremental_refresh_computing_resource
     */
    INCREMENTAL_GUC_HG_INCREMENTAL_REFRESH_COMPUTING_RESOURCE("incremental_guc_hg_incremental_refresh_computing_resource", true),

    /**
     * incremental_guc_hg_incremental_serverless_computing_required_cores
     */
    INCREMENTAL_GUC_HG_INCREMENTAL_SERVERLESS_COMPUTING_REQUIRED_CORES("incremental_guc_hg_incremental_serverless_computing_required_cores", true),

    /**
     * incremental_state_table_group
     */
    INCREMENTAL_STATE_TABLE_GROUP("incremental_state_table_group", true),

    /**
     * batch_auto_refresh_schd_start_time
     */
    BATCH_AUTO_REFRESH_SCHD_START_TIME("batch_auto_refresh_schd_start_time", true),

    /**
     * batch_auto_refresh_interval
     */
    BATCH_AUTO_REFRESH_INTERVAL("batch_auto_refresh_interval", true),

    /**
     * batch_guc_hg_batch_refresh_computing_resource
     */
    BATCH_GUC_HG_BATCH_REFRESH_COMPUTING_RESOURCE("batch_guc_hg_batch_refresh_computing_resource", true),

    /**
     * batch_guc_hg_batch_serverless_computing_required_cores
     */
    BATCH_GUC_HG_BATCH_SERVERLESS_COMPUTING_REQUIRED_CORES("batch_guc_hg_batch_serverless_computing_required_cores", true),

    /**
     * streaming_auto_refresh_schd_start_time
     */
    STREAMING_AUTO_REFRESH_SCHD_START_TIME("streaming_auto_refresh_schd_start_time", true),

    /**
     * streaming_state_table_group
     */
    STREAMING_STATE_TABLE_GROUP("streaming_state_table_group", true),

    /**
     * streaming_state_cu
     */
    STREAMING_STATE_CU("streaming_state_cu", true),

    /**
     * streaming_state_parallelism
     */
    STREAMING_STATE_PARALLELISM("streaming_state_parallelism", true),

    /**
     * auto_partitioning_enable
     */
    AUTO_PARTITIONING_ENABLE("auto_partitioning_enable", true),

    /**
     * auto_partitioning_time_unit
     */
    AUTO_PARTITIONING_TIME_UNIT("auto_partitioning_time_unit", true),

    /**
     * auto_partitioning_time_zone
     */
    AUTO_PARTITIONING_TIME_ZONE("auto_partitioning_time_zone", true),

    /**
     * auto_partitioning_num_precreate
     */
    AUTO_PARTITIONING_NUM_PRECREATE("auto_partitioning_num_precreate", true),

    /**
     * auto_partitioning_num_prerefresh
     */
    AUTO_PARTITIONING_NUM_PREREFRESH("auto_partitioning_num_prerefresh", true),

    /**
     * auto_partitioning_num_retention
     */
    AUTO_PARTITIONING_NUM_RETENTION("auto_partitioning_num_retention", true),

    /**
     * auto_partitioning_num_hot
     */
    AUTO_PARTITIONING_NUM_HOT("auto_partitioning_num_hot", true),

    /**
     * auto_partitioning_schd_start_time
     */
    AUTO_PARTITIONING_SCHD_START_TIME("auto_partitioning_schd_start_time", true),

    /**
     * refresh_guc_
     */
    REFRESH_GUC_PREFIX("refresh_guc_", true, true),

    //////////////dynamic table key end

    /**
     * storage_mode
     */
    STORAGE_MODE("storage_mode", true),
    /**
     * 是否外表
     */
    FOREIGN("foreign", false),

    /**
     * 连接外部数据源的服务器
     */
    SERVER_NAME("server_name", false),

    /**
     * 任务定义
     */
    TASK_DEFINITION("task_definition", true),

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

    HologresPropertyKey(String value) {
        this(value, false);
    }

    HologresPropertyKey(String value, boolean supportPrint) {
        this(value, supportPrint, false);
    }

    HologresPropertyKey(String value, boolean supportPrint, boolean prefix) {
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

    public static HologresPropertyKey getByValue(String value) {
        HologresPropertyKey[] holoPropertyKeys = HologresPropertyKey.values();
        for (HologresPropertyKey m : holoPropertyKeys) {
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
