package com.aliyun.fastmodel.transform.hive.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;

/**
 * FieldsTerminated
 *
 * @author panguanjing
 * @date 2023/2/6
 */
public class FieldsTerminated extends BaseClientProperty<String> {

    public FieldsTerminated() {
        this.setKey(HivePropertyKey.FIELDS_TERMINATED.getValue());
    }

    @Override
    public String valueString() {
        return value;
    }

    @Override
    public void setValueString(String value) {
        this.value = value;
    }
}
