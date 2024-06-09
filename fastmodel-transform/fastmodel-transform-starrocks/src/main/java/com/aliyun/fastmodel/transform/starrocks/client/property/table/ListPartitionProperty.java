package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.alibaba.fastjson.JSON;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ListClientPartition;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2023/12/25
 */
public class ListPartitionProperty extends BaseClientProperty<ListClientPartition> {

    public ListPartitionProperty() {
        this.setKey(StarRocksProperty.TABLE_LIST_PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        this.setValue(JSON.parseObject(value, ListClientPartition.class));
    }
}
