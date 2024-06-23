package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.PARTITION_LIVE_NUMBER;

/**
 * @author 子梁
 * @date 2024/1/5
 */
public class PartitionLiveNumberProperty extends BaseClientProperty<Integer> {

    public PartitionLiveNumberProperty() {
        setKey(PARTITION_LIVE_NUMBER.getValue());
    }

    @Override
    public String valueString() {
        return String.valueOf(value);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        this.value = Integer.parseInt(value);
    }

}
