package com.aliyun.fastmodel.transform.api.extension.client.property.index;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_INDEX_TYPE;

/**
 * 索引描述信息属性
 *
 * @author panguanjing
 * @date 2023/12/18
 */
public class IndexTypeProperty extends BaseClientProperty<String> {

    public IndexTypeProperty() {
        setKey(TABLE_INDEX_TYPE.getValue());
    }

    @Override
    public String valueString() {
        return value;
    }

    @Override
    public void setValueString(String value) {
        this.setValue(value);
    }
}
