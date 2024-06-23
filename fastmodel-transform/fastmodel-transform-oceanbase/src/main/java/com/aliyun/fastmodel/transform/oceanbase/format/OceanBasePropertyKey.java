package com.aliyun.fastmodel.transform.oceanbase.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyValueType;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * OceanBasePropertyKey
 *
 * @author panguanjing
 * @date 2024/1/20
 */
@Getter
public enum OceanBasePropertyKey implements PropertyKey {
    /**
     * on update
     */
    SORT_KEY("sortkey"),

    /**
     * table mode
     */
    TABLE_MODE("table_mode"),

    /**
     * DUPLICATE_SCOPE
     */
    DUPLICATE_SCOPE("duplicate_scope"),

    /**
     * comment
     */
    COMMENT("comment"),

    /**
     * COMPRESSION
     */
    COMPRESSION("compression"),

    /**
     * charset key
     */
    CHARSET_KEY("charset_key", PropertyValueType.IDENTIFIER),

    /**
     * collate
     */
    COLLATE("collate", PropertyValueType.IDENTIFIER),

    /**
     * row format
     */
    ROW_FORMAT("row_format", PropertyValueType.IDENTIFIER),

    /**
     * PCTFREE
     */
    PCTFREE("pctfree", PropertyValueType.NUMBER_LITERAL),

    /**
     * progressive_merge_num
     */
    PROGRESSIVE_MERGE_NUM("progressive_merge_num", PropertyValueType.NUMBER_LITERAL),
    /**
     * block size
     */
    BLOCK_SIZE("block_size", PropertyValueType.NUMBER_LITERAL),
    /**
     * table id
     */
    TABLE_ID("table_id", PropertyValueType.NUMBER_LITERAL),
    /**
     * replica num
     */
    REPLICA_NUM("replica_num", PropertyValueType.NUMBER_LITERAL),
    /**
     * storage format version
     */
    STORAGE_FORMAT_VERSION("storage_format_version", PropertyValueType.NUMBER_LITERAL),
    /**
     * tablet size
     */
    TABLET_SIZE("tablet_size", PropertyValueType.NUMBER_LITERAL),
    /**
     * id
     */
    MAX_USED_PART_ID("max_used_part_id", PropertyValueType.NUMBER_LITERAL),

    /**
     * PARALLEL
     */
    PARALLEL("parallel", PropertyValueType.NUMBER_LITERAL),

    /**
     * Partition
     */
    PARTITION("partition", PropertyValueType.EXPRESSION);

    private final String value;

    private final boolean supportPrint;

    private final PropertyValueType valueType;

    OceanBasePropertyKey(String value) {
        this(value, true, PropertyValueType.STRING_LITERAL);
    }

    OceanBasePropertyKey(String value, PropertyValueType propertyValueType) {
        this(value, true, propertyValueType);
    }

    OceanBasePropertyKey(String value, boolean supportPrint, PropertyValueType valueType) {
        this.value = value;
        this.supportPrint = supportPrint;
        this.valueType = valueType;
    }

    public static OceanBasePropertyKey getByValue(String value) {
        OceanBasePropertyKey[] keys = OceanBasePropertyKey.values();
        for (OceanBasePropertyKey oceanBasePropertyKey : keys) {
            if (StringUtils.equalsIgnoreCase(oceanBasePropertyKey.getValue(), value)) {
                return oceanBasePropertyKey;
            }
        }
        return null;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public boolean isSupportPrint() {
        return this.supportPrint;
    }
}
