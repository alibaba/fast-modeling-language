package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_DISTRIBUTED_BUCKETS;

/**
 * 分桶列的个数
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class DistributeBucketsNum extends BaseClientProperty<Integer> {
    public DistributeBucketsNum() {
        setKey(TABLE_DISTRIBUTED_BUCKETS.getValue());
    }

    @Override
    public String valueString() {
        return String.valueOf(value);
    }

    @Override
    public void setValueString(String value) {
        if (!StringUtils.isNumeric(value)) {
            return;
        }
        this.value = Integer.parseInt(value);
    }
}
