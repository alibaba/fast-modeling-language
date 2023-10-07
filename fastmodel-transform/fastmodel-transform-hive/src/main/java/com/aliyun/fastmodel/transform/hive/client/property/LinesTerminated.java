package com.aliyun.fastmodel.transform.hive.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;

/**
 * LinesTerminated
 *
 * @author panguanjing
 * @date 2023/2/6
 */
public class LinesTerminated extends BaseClientProperty<String> {

    public LinesTerminated() {
        this.setKey(HivePropertyKey.LINES_TERMINATED.getValue());
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
