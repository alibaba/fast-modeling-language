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
    SORT_KEY("SORTKEY"),

    /**
     * table mode
     */
    TABLE_MODE("TABLE_MODE"),

    /**
     * DUPLICATE_SCOPE
     */
    DUPLICATE_SCOPE("DUPLICATE_SCOPE"),

    /**
     * comment
     */
    COMMENT("COMMENT"),

    /**
     * COMPRESSION
     */
    COMPRESSION("COMPRESSION"),

    /**
     * charset key
     */
    CHARSET_KEY("CHARSET_KEY", PropertyValueType.IDENTIFIER),

    /**
     * collate
     */
    COLLATE("COLLATE", PropertyValueType.IDENTIFIER),

    /**
     * row format
     */
    ROW_FORMAT("ROW_FORMAT", PropertyValueType.IDENTIFIER),

    /**
     * PCTFREE
     */
    PCTFREE("PCTFREE", PropertyValueType.NUMBER_LITERAL),

    /**
     * progressive_merge_num
     */
    PROGRESSIVE_MERGE_NUM("PROGRESSIVE_MERGE_NUM", PropertyValueType.NUMBER_LITERAL),
    /**
     * block size
     */
    BLOCK_SIZE("BLOCK_SIZE", PropertyValueType.NUMBER_LITERAL),
    /**
     * table id
     */
    TABLE_ID("TABLE_ID", PropertyValueType.NUMBER_LITERAL),
    /**
     * replica num
     */
    REPLICA_NUM("REPLICA_NUM", PropertyValueType.NUMBER_LITERAL),
    /**
     * storage format version
     */
    STORAGE_FORMAT_VERSION("STORAGE_FORMAT_VERSION", PropertyValueType.NUMBER_LITERAL),
    /**
     * tablet size
     */
    TABLET_SIZE("TABLET_SIZE", PropertyValueType.NUMBER_LITERAL),
    /**
     * id
     */
    MAX_USED_PART_ID("MAX_USED_PART_ID", PropertyValueType.NUMBER_LITERAL),

    /**
     * PARALLEL
     */
    PARALLEL("PARALLEL", PropertyValueType.NUMBER_LITERAL),

    /**
     * Partition
     */
    PARTITION("PARTITION", false, PropertyValueType.EXPRESSION),

    /**
     * use bloom filter
     */
    USE_BLOOM_FILTER("USE_BLOOM_FILTER", PropertyValueType.EXPRESSION),

    ;

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
