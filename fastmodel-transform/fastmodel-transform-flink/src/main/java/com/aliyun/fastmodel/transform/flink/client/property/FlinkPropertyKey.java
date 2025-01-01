package com.aliyun.fastmodel.transform.flink.client.property;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/5/22
 */
public enum FlinkPropertyKey implements PropertyKey {

    /**
     * watermark
     */
    WATERMARK_EXPRESSION("watermark_expression")
    ;
    private String value;

    private boolean supportPrint;

    FlinkPropertyKey(String value) {
        this(value, false);
    }

    FlinkPropertyKey(String value, boolean supportPrint) {
        this.value = value;
        this.supportPrint = supportPrint;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isSupportPrint() {
        return supportPrint;
    }

    public static FlinkPropertyKey getByValue(String value) {
        FlinkPropertyKey[] flinkPropertyKeys = FlinkPropertyKey.values();
        for (FlinkPropertyKey m : flinkPropertyKeys) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
        }
        return null;
    }
}
