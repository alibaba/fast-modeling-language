package com.aliyun.fastmodel.transform.mc.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/19
 */
public class AcidDataRetainHours extends BaseClientProperty<String> {

    public AcidDataRetainHours() {
        this.setKey(MaxComputePropertyKey.ACID_DATA_RETAIN_HOURS.getValue());
    }

    @Override
    public String valueString() {
        return value;
    }

    @Override
    public void setValueString(String value) {
        this.value = value;
    }
}
