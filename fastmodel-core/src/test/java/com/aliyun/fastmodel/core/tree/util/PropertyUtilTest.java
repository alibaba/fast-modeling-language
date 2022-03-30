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

import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/9
 */
public class PropertyUtilTest {

    @Test
    public void testEqual() {
        Property property = new Property("a", "b");
        Property property1 = new Property("a", "b");
        assertEquals(property, property1);
    }

    @Test
    public void testToObject() {
        Property property = new Property("name", new StringLiteral("jw"));
        Property property1 = new Property("age", new LongLiteral("1"));
        Property property2 = new Property("home_address", new String("hN"));
        Property marry = new Property("marry", new BooleanLiteral("true"));
        List<Property> list = ImmutableList.of(property1, property, property2, marry);
        Person person = PropertyUtil.toObject(list, Person.class);
        assertEquals(person.getAge(), Integer.valueOf(property1.getValue()));
        assertEquals(person.getName(), property.getValue());
        assertEquals(person.getHomeAddress(), property2.getValue());
        assertTrue(person.isMarry());
    }

    @Getter
    @Setter
    static class Person {
        private String name;
        private Integer age;
        private String homeAddress;
        private boolean marry;
    }
}