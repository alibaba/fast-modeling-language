package com.aliyun.fastmodel.transform.starrocks.client.property.column;

import java.util.Locale;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.AggDesc;

/**
 * aggr column property
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class AggrColumnProperty extends BaseClientProperty<AggDesc> {

    public AggrColumnProperty() {
        this.setKey(StarRocksProperty.COLUMN_AGG_DESC.getValue());
    }

    @Override
    public String valueString() {
        return value.name();
    }

    @Override
    public void setValueString(String value) {
        this.value = AggDesc.valueOf(value.toUpperCase(Locale.ROOT));
    }
}
