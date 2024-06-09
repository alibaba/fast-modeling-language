package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.alibaba.fastjson.JSON;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.TimeExpressionClientPartition;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2023/12/26
 */
public class TimeExpressionPartitionProperty extends BaseClientProperty<TimeExpressionClientPartition> {

    public TimeExpressionPartitionProperty() {
        this.setKey(StarRocksProperty.TABLE_EXPRESSION_PARTITION.getValue());
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
        this.setValue(JSON.parseObject(value, TimeExpressionClientPartition.class));
    }
}
