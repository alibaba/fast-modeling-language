package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;

/**
 * 分区定义的文本化描述
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class TablePartitionRaw extends BaseClientProperty<String> {

    public TablePartitionRaw() {
        this.setKey(StarRocksProperty.TABLE_PARTITION_RAW.getValue());
    }

    @Override
    public String valueString() {
        return this.value;
    }

    @Override
    public void setValueString(String value) {
        this.value = value;
    }
}
