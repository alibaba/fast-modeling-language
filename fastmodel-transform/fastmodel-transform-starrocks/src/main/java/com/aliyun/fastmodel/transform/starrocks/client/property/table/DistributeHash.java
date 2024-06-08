package com.aliyun.fastmodel.transform.starrocks.client.property.table;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * 分桶列的个数
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class DistributeHash extends BaseClientProperty<List<String>> {

    public DistributeHash() {
        setKey(StarRocksProperty.TABLE_DISTRIBUTED_HASH.getValue());
    }

    @Override
    public String valueString() {
        return String.join(",", value);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
       this.value = Lists.newArrayList(StringUtils.split(value, ","));
    }
}
