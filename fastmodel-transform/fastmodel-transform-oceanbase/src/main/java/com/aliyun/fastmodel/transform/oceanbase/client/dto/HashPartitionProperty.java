package com.aliyun.fastmodel.transform.oceanbase.client.dto;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.oceanbase.client.dto.hash.HashPartitionClient;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;

/**
 * hash partition property
 *
 * @author panguanjing
 * @date 2024/2/22
 */
public class HashPartitionProperty extends BaseClientProperty<HashPartitionClient> {

    public HashPartitionProperty() {
        setKey(OceanBasePropertyKey.PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(JSON.parseObject(value, HashPartitionClient.class));
    }
}
