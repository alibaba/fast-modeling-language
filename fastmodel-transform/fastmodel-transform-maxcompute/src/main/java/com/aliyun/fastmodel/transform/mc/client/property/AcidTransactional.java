package com.aliyun.fastmodel.transform.mc.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import org.apache.commons.lang3.BooleanUtils;

/**
 * trnsactional
 *
 * @author panguanjing
 * @date 2023/2/19
 */
public class AcidTransactional extends BaseClientProperty<Boolean> {

    public AcidTransactional() {
        this.setKey(MaxComputePropertyKey.TRANSACTIONAL.getValue());
    }

    @Override
    public String valueString() {
        return BooleanUtils.toStringTrueFalse(this.getValue());
    }

    @Override
    public void setValueString(String value) {
        this.setValue(BooleanUtils.toBoolean(value));
    }
}
