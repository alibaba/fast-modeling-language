package com.aliyun.fastmodel.transform.starrocks.format;

import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * StarRocksProperty
 * $$是代表系统属性，不会进行输出
 *
 * @author panguanjing
 * @date 2023/9/14
 */
public enum StarRocksProperty implements PropertyKey {

    /**
     * rollup
     */
    TABLE_ROLLUP("rollup"),

    /**
     * char set
     */
    COLUMN_CHAR_SET("char_set");

    private final String value;

    private final boolean supportPrint;

    StarRocksProperty(String value) {
        this(value, false);
    }

    StarRocksProperty(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static PropertyKey getByValue(String value) {
        StarRocksProperty[] starRocksProperties = StarRocksProperty.values();
        for (StarRocksProperty starRocksProperty : starRocksProperties) {
            if (StringUtils.equalsIgnoreCase(starRocksProperty.getValue(), value)) {
                return starRocksProperty;
            }
        }
        for (ExtensionPropertyKey extensionPropertyKey : ExtensionPropertyKey.values()) {
            if (StringUtils.equalsIgnoreCase(extensionPropertyKey.getValue(), value)) {
                return extensionPropertyKey;
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
