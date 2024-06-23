package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_RANGE_PARTITION;

/**
 * single range partition
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class SingleRangePartitionProperty extends BaseClientProperty<SingleRangeClientPartition> {

    public SingleRangePartitionProperty() {
        this.setKey(TABLE_RANGE_PARTITION.getValue());
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
