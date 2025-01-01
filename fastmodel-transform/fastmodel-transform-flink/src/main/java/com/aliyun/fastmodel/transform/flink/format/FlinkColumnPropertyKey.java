package com.aliyun.fastmodel.transform.flink.format;

import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/5/16
 */
public enum FlinkColumnPropertyKey implements PropertyKey {

    /**
     * METADATA
     */
    METADATA("metadata"),

    /**
     * VIRTUAL
     */
    VIRTUAL("virtual"),

    /**
     * METADATA USE KEY
     */
    METADATA_KEY("metadata_key"),

    /**
     * COMPUTED COLUMN
     */
    COMPUTED("computed"),

    /**
     * COMPUTED COLUMN EXPRESSION
     */
    COMPUTED_COLUMN_EXPRESSION("computed_column_expression"),
    ;

    @Getter
    private final String value;

    @Getter
    private final boolean supportPrint;

    FlinkColumnPropertyKey(String value) {
        this(value, false);
    }

    FlinkColumnPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static PropertyKey getByValue(String value) {
        FlinkColumnPropertyKey[] flinkColumnPropertyKeys = FlinkColumnPropertyKey.values();
        for (FlinkColumnPropertyKey flinkColumnPropertyKey : flinkColumnPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(flinkColumnPropertyKey.getValue(), value)) {
                return flinkColumnPropertyKey;
            }
        }
        for (ExtensionPropertyKey extensionPropertyKey : ExtensionPropertyKey.values()) {
            if (StringUtils.equalsIgnoreCase(extensionPropertyKey.getValue(), value)) {
                return extensionPropertyKey;
            }
        }
        return null;
    }
}
