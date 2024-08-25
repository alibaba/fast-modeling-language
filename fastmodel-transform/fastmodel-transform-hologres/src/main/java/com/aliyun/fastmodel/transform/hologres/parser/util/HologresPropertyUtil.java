package com.aliyun.fastmodel.transform.hologres.parser.util;

import java.util.List;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresPropertyConverter;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.google.common.collect.Lists;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/4/16
 */
public class HologresPropertyUtil {

    public static String getPropertyValue(HologresVersion hologresVersion, String key, String value) {
        HologresPropertyConverter instance = HologresPropertyConverter.getInstance();
        BaseClientProperty baseClientProperty = instance.create(key, value);
        //https://help.aliyun.com/document_detail/160754.html?spm=a2c4g.467951.0.0.12c01598oEHfQG
        List<String> list = baseClientProperty.toColumnList();
        if (list.isEmpty()) {
            return value;
        }
        if (hologresVersion == HologresVersion.V2) {
            //按照2.0的方式，call set_table_property('tbl', 'clustering_key', '"C1",c2');
            String[] strings = list.stream().map(l -> {
                if (l.startsWith(StripUtils.DOUBLE_QUOTE)) {
                    return l;
                } else {
                    return StripUtils.addDoubleStrip(l);
                }
            }).toArray(String[]::new);
            baseClientProperty.setColumnList(Lists.newArrayList(strings));
            value = baseClientProperty.valueString();
        } else {
            //默认按照1.0的方式
            if (!value.startsWith(StripUtils.DOUBLE_QUOTE)) {
                value = StripUtils.addDoubleStrip(value);
            }
        }
        return value;
    }
}
