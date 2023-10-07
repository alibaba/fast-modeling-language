/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.core.tree.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.PropertyConvertException;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

/**
 * @author panguanjing
 * @date 2020/11/9
 */
public class PropertyUtil {

    public static Map<String, String> toMap(List<Property> propertyList) {
        if (propertyList == null) {
            return Maps.newLinkedHashMap();
        }
        Map<String, String> maps = new LinkedHashMap<>(propertyList.size());
        for (Property property : propertyList) {
            maps.put(property.getName(), property.getValue());
        }
        return maps;
    }

    public static List<Property> toProperty(Map<String, String> map) {
        if (map == null) {
            return ImmutableList.of();
        }
        List<Property> list = Lists.newArrayListWithCapacity(map.size());
        for (String k : map.keySet()) {
            Property property = new Property(k, map.get(k));
            list.add(property);
        }
        return list;
    }

    /**
     * 转换为对象处理
     *
     * @param properties
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T toObject(List<Property> properties, Class<T> clazz) throws PropertyConvertException {
        Map<String, BaseLiteral> baseLiteralMap = Maps.newHashMap();
        for (Property p : properties) {
            baseLiteralMap.put(p.getName().toUpperCase(), p.getValueLiteral());
        }
        Field[] declaredFields = clazz.getDeclaredFields();
        try {
            T t = clazz.newInstance();
            for (Field f : declaredFields) {
                Class<?> type = f.getType();
                String s = UnderLineUtils.humpToUnderLine(f.getName());
                BaseLiteral baseLiteral = baseLiteralMap.get(s.toUpperCase());
                if (baseLiteral == null) {
                    continue;
                }
                String prefix = "set";
                if (type == String.class) {
                    if (!(baseLiteral instanceof StringLiteral)) {
                        continue;
                    }
                    StringLiteral stringLiteral = (StringLiteral)baseLiteral;
                    Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), String.class);
                    m.invoke(t, new Object[] {stringLiteral.getValue()});
                } else if (type == Long.class || type == long.class || type == Integer.class || type == int.class) {
                    if (baseLiteral.getClass() != LongLiteral.class) {
                        continue;
                    }
                    Long literal = ((LongLiteral)baseLiteral).getValue();
                    if (type == Long.class || type == long.class) {
                        Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), Long.class);
                        m.invoke(t, literal);
                    } else if (type == Integer.class || type == int.class) {
                        Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()),
                            Integer.class);
                        m.invoke(t, literal.intValue());
                    }
                } else if (type == Boolean.class || type == boolean.class) {
                    if (baseLiteral.getClass() != BooleanLiteral.class) {
                        continue;
                    }
                    Method m = null;
                    if (type == boolean.class) {
                        m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), boolean.class);
                    } else if (type == Boolean.class) {
                        m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), Boolean.class);
                    }
                    boolean booleanLiteral = ((BooleanLiteral)baseLiteral).isValue();
                    m.invoke(t, booleanLiteral);
                } else if (type.isEnum()) {
                    if (!(baseLiteral instanceof StringLiteral)) {
                        continue;
                    }
                    StringLiteral stringLiteral = (StringLiteral)baseLiteral;
                    Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), type);
                    Class<Enum> e = (Class<Enum>)type;
                    m.invoke(t, new Object[] {Enum.valueOf(e, stringLiteral.getValue())});
                } else if (type == List.class) {
                    Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), List.class);
                    if (baseLiteral instanceof ListStringLiteral) {
                        ListStringLiteral listStringLiteral = (ListStringLiteral)baseLiteral;
                        List<StringLiteral> stringLiteralList = listStringLiteral.getStringLiteralList();
                        m.invoke(t, stringLiteralList.stream().map(x -> x.getValue()).collect(Collectors.toList()));
                    } else if (baseLiteral instanceof StringLiteral) {
                        StringLiteral stringLiteral = (StringLiteral)baseLiteral;
                        m.invoke(t, Arrays.asList(stringLiteral.getValue()));
                    }
                } else if (type == URL.class) {
                    if (!(baseLiteral instanceof StringLiteral)) {
                        continue;
                    }
                    Method m = clazz.getDeclaredMethod(prefix + StringUtils.capitalize(f.getName()), URL.class);
                    StringLiteral stringLiteral = (StringLiteral)baseLiteral;
                    m.invoke(t, new URL(null, stringLiteral.getValue(), new EngineUrlStreamHandler()));
                }
            }
            return t;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException | MalformedURLException e) {
            throw new PropertyConvertException("toObject error:" + clazz, e);
        }

    }
}
