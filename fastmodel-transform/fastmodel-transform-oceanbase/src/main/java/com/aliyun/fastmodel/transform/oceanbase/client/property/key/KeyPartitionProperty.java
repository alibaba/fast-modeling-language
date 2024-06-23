package com.aliyun.fastmodel.transform.oceanbase.client.property.key;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;

/**
 * KeyPartitionProperty
 *
 * @author panguanjing
 * @date 2024/2/22
 */
public class KeyPartitionProperty extends BaseClientProperty<KeyPartitionClient> {

    public KeyPartitionProperty() {
        setKey(OceanBasePropertyKey.PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(JSON.parseObject(value, KeyPartitionClient.class));
    }
}
