package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_REPLICATION_NUM;

/**
 * ReplicationNum
 *
 * @author panguanjing
 * @date 2023/9/22
 */
public class ReplicationNum extends BaseClientProperty<Integer> {
    public ReplicationNum() {
        setKey(TABLE_REPLICATION_NUM.getValue());
    }

    @Override
    public String valueString() {
        return String.valueOf(value);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        this.value = Integer.parseInt(value);
    }
}
