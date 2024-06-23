package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;

/**
 * 分区定义的文本化描述
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class TablePartitionRaw extends BaseClientProperty<String> {

    public TablePartitionRaw() {
        this.setKey(TABLE_PARTITION_RAW.getValue());
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
