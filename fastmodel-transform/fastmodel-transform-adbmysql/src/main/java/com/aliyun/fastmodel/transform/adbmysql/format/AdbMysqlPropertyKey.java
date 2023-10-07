package com.aliyun.fastmodel.transform.adbmysql.format;

import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/11
 */
public enum AdbMysqlPropertyKey implements PropertyKey {

    /**
     * partition_date_format
     */
    PARTITION_DATE_FORMAT("adb_mysql.partition_date_format"),
    /**
     * 生命周期
     */
    LIFE_CYCLE("adb_mysql.life_cycle"),

    /**
     * distribute by
     */
    DISTRIBUTED_BY("adb_mysql.distributed_by"),

    /**
     * storage policy
     */
    STORAGE_POLICY("adb_mysql.storage_policy"),

    /**
     * hot partition count
     */
    HOT_PARTITION_COUNT("adb_mysql.hot_partition_count"),

    /**
     * block_size
     */
    BLOCK_SIZE("adb_mysql.block_size");

    @Getter
    private final String value;

    @Getter
    private final boolean supportPrint;

    AdbMysqlPropertyKey(String value) {
        this(value, false);
    }

    AdbMysqlPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }


    public static AdbMysqlPropertyKey getByValue(String value) {
        if (!StringUtils.startsWithIgnoreCase(value, Constants.ADB_MYSQL)) {
            return null;
        }
        AdbMysqlPropertyKey[] values = AdbMysqlPropertyKey.values();
        for (AdbMysqlPropertyKey adbMysqlPropertyKey : values) {
            if (StringUtils.equalsIgnoreCase(adbMysqlPropertyKey.getValue(), value)) {
                return adbMysqlPropertyKey;
            }
        }
        return null;
    }

}
