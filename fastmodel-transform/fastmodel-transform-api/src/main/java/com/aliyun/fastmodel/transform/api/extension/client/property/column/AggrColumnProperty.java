package com.aliyun.fastmodel.transform.api.extension.client.property.column;

import java.util.Locale;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.extension.tree.column.AggregateDesc;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AGG_DESC;

/**
 * aggr column property
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class AggrColumnProperty extends BaseClientProperty<AggregateDesc> {

    public AggrColumnProperty() {
        this.setKey(COLUMN_AGG_DESC.getValue());
    }

    @Override
    public String valueString() {
        return value.name();
    }

    @Override
    public void setValueString(String value) {
        this.value = AggregateDesc.valueOf(value.toUpperCase(Locale.ROOT));
    }
}
