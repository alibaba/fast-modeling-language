package com.aliyun.fastmodel.transform.mc.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/19
 */
public class AcidWriteBucketNum extends BaseClientProperty<Integer> {
    public AcidWriteBucketNum() {
        this.setKey(MaxComputePropertyKey.WRITE_BUCKET_NUM.getValue());
    }

    @Override
    public String valueString() {
        return String.valueOf(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(Integer.parseInt(value));
    }
}
