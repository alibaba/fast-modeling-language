package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/1/5
 */
public class PartitionLiveNumberProperty extends BaseClientProperty<Integer> {

    public PartitionLiveNumberProperty() {
        setKey(StarRocksProperty.PARTITION_LIVE_NUMBER.getValue());
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
