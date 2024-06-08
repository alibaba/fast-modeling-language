package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * 分桶列的个数
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class DistributeBucketsNum extends BaseClientProperty<Integer> {
    public DistributeBucketsNum() {
        setKey(StarRocksProperty.TABLE_DISTRIBUTED_BUCKETS.getValue());
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
