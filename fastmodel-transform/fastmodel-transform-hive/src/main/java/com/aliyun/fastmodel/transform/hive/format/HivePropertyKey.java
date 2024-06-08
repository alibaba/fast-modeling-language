package com.aliyun.fastmodel.transform.hive.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * hive property key
 *
 * @author panguanjing
 * @date 2023/2/6
 */
public enum HivePropertyKey implements PropertyKey {

    /**
     * storage format
     */
    STORAGE_FORMAT("hive.storage_format"),

    /**
     * hive.fields.terminated
     */
    FIELDS_TERMINATED("hive.fields_terminated"),

    /**
     * hive.lines.terminated
     */
    LINES_TERMINATED("hive.lines_terminated"),

    /**
     * external table
     */
    EXTERNAL_TABLE("hive.table_external"),

    /**
     * location
     */
    LOCATION("hive.location"),

    /**
     * row format serde
     */
    ROW_FORMAT_SERDE("hive.row_format_serde"),

    /**
     * STORED AS INPUTFORMAT
     */
    STORED_INPUT_FORMAT("hive.stored_input_format"),

    /**
     * STORED AS OUTPUTFORMAT
     */
    STORED_OUTPUT_FORMAT("hive.stored_output_format"),

    /**
     * WITH SERDEPROPERTIES
     */
    SERDE_PROPS("hive.serde_props")

    ;

    /**
     * 是否需要print
     */
    private final String value;

    /**
     * 是否需要print
     */
    @Getter
    private final boolean supportPrint;

    HivePropertyKey(String value) {this(value, false);}

    HivePropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    @Override
    public String getValue() {
        return value;
    }

    /**
     * 根据value查询property key
     *
     * @param value
     * @return
     */
    public static HivePropertyKey getByValue(String value) {
        HivePropertyKey[] hivePropertyKeys = HivePropertyKey.values();
        for (HivePropertyKey hivePropertyKey : hivePropertyKeys) {
            if (StringUtils.equalsIgnoreCase(hivePropertyKey.getValue(), value)) {
                return hivePropertyKey;
            }
        }
        return null;
    }

}
