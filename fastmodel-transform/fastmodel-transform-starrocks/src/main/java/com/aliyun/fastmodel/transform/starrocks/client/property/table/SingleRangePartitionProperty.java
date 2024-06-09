package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * single range partition
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class SingleRangePartitionProperty extends BaseClientProperty<SingleRangeClientPartition> {

    public SingleRangePartitionProperty() {
        this.setKey(StarRocksProperty.TABLE_RANGE_PARTITION.getValue());
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
        this.setValue(JSON.parseObject(value, SingleRangeClientPartition.class));
    }
}
