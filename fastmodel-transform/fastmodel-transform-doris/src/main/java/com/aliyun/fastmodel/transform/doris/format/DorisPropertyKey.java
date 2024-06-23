package com.aliyun.fastmodel.transform.doris.format;

import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * DorisPropertyKey
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public enum DorisPropertyKey implements PropertyKey {
    /**
     * on update
     */
    COLUMN_ON_UPDATE_CURRENT_TIMESTAMP("column_on_update");

    private final String value;

    private final boolean supportPrint;

    DorisPropertyKey(String value) {
        this(value, false);
    }

    DorisPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    public static PropertyKey getByValue(String value) {
        DorisPropertyKey[] propertyKeys = DorisPropertyKey.values();
        for (DorisPropertyKey starRocksProperty : propertyKeys) {
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
        return this.value;
    }

    @Override
    public boolean isSupportPrint() {
        return this.supportPrint;
    }
}
