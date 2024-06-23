package com.aliyun.fastmodel.transform.oceanbase.client.property.range;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;

/**
 * RangePartitionProperty
 *
 * @author panguanjing
 * @date 2024/2/22
 */
public class RangePartitionProperty extends BaseClientProperty<RangePartitionClient> {

    public RangePartitionProperty() {
        setKey(OceanBasePropertyKey.PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        this.value = JSON.parseObject(value, RangePartitionClient.class);
    }
}
