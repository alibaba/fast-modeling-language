package com.aliyun.fastmodel.transform.mc.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import org.apache.commons.lang3.BooleanUtils;

/**
 * acid.incremental.query.out.of.time.range.enabled
 *
 * @author panguanjing
 * @date 2023/2/20
 */
public class AcidIncrementalQueryOutOfTimeRangeEnabled extends BaseClientProperty<Boolean> {

    public AcidIncrementalQueryOutOfTimeRangeEnabled() {
        this.setKey(MaxComputePropertyKey.ACID_INCREMENTAL_QUERY_OUT_OF_TIME_RANGE_ENABLED.getValue());
    }

    @Override
    public String valueString() {
        return BooleanUtils.toStringTrueFalse(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(BooleanUtils.toBoolean(value));
    }
}
