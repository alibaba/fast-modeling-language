package com.aliyun.fastmodel.transform.mc.client.property;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;

/**
 * AutoPartitionedByProperty
 * @author panguanjing
 * @date 2025/12/7
 */
public class AutoPartitionedByProperty extends BaseClientProperty<AutoPartitionedByClient> {

    public AutoPartitionedByProperty() {
        this.setKey(MaxComputePropertyKey.AUTO_PARTITIONED_BY.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(JSON.parseObject(value, AutoPartitionedByClient.class));
    }
}
