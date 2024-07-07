package com.aliyun.fastmodel.transform.adbmysql.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyValueType;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/11
 */
@Getter
public enum AdbMysqlPropertyKey implements PropertyKey {

    /**
     * 生命周期
     */
    LIFE_CYCLE("LIFE_CYCLE"),

    /**
     * distribute by
     */
    DISTRIBUTE_BY("DISTRIBUTE_BY"),

    /**
     * storage policy
     */
    STORAGE_POLICY("STORAGE_POLICY", true),

    /**
     * hot partition count
     */
    HOT_PARTITION_COUNT("HOT_PARTITION_COUNT", true, PropertyValueType.NUMBER_LITERAL),

    /**
     * block_size
     */
    BLOCK_SIZE("BLOCK_SIZE", true, PropertyValueType.NUMBER_LITERAL),
    /**
     * index all
     */
    INDEX_ALL("INDEX_ALL", true),
    /**
     * engine
     */
    ENGINE("ENGINE", true),
    /**
     * table properties
     */
    TABLE_PROPERTIES("TABLE_PROPERTIES", true),
    ;

    private final String value;

    private final boolean supportPrint;

    private final PropertyValueType valueType;

    AdbMysqlPropertyKey(String value) {
        this(value, false);
    }

    AdbMysqlPropertyKey(String value, boolean supportPrint, PropertyValueType valueType) {
        this.value = value;
        this.supportPrint = supportPrint;
        this.valueType = valueType;
    }

    AdbMysqlPropertyKey(String value, boolean supportPrint) {
        this(value, supportPrint, PropertyValueType.STRING_LITERAL);
    }

    public static AdbMysqlPropertyKey getByValue(String value) {
        AdbMysqlPropertyKey[] values = AdbMysqlPropertyKey.values();
        for (AdbMysqlPropertyKey adbMysqlPropertyKey : values) {
            if (StringUtils.equalsIgnoreCase(adbMysqlPropertyKey.getValue(), value)) {
                return adbMysqlPropertyKey;
            }
        }
        return null;
    }

}
