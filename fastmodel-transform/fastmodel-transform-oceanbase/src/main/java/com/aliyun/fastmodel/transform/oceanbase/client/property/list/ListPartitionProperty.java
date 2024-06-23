package com.aliyun.fastmodel.transform.oceanbase.client.property.list;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBasePropertyKey;

/**
 * list partition property
 *
 * @author panguanjing
 * @date 2024/2/26
 */
public class ListPartitionProperty extends BaseClientProperty<ListPartitionClient> {

    public ListPartitionProperty() {
        setKey(OceanBasePropertyKey.PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        this.setValue(JSON.parseObject(value, ListPartitionClient.class));
    }
}
