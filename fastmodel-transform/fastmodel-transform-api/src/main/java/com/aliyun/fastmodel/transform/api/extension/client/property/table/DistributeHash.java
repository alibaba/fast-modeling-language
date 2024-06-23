package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_DISTRIBUTED_HASH;

/**
 * 分桶列的个数
 *
 * @author panguanjing
 * @date 2023/9/20
 */
public class DistributeHash extends BaseClientProperty<List<String>> {

    public DistributeHash() {
        setKey(TABLE_DISTRIBUTED_HASH.getValue());
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
