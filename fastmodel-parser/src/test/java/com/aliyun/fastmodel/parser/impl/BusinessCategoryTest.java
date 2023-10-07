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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.tree.statement.businesscategory.CreateBusinessCategory;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.DropBusinessCategory;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.SetBusinessCategoryComment;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.SetBusinessCategoryProperties;
import com.aliyun.fastmodel.core.tree.statement.businesscategory.UnSetBusinessCategoryProperties;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * business Category Test
 *
 * @author panguanjing
 * @date 2021/8/13
 */
public class BusinessCategoryTest extends BaseTest {

    @Test
    public void testCreate() {
        String createFml = "Create business_category c1 alias 'alias' comment 'comment';";
        CreateBusinessCategory parse = parse(createFml, CreateBusinessCategory.class);
        assertEquals(parse.toString(), "CREATE BUSINESS_CATEGORY c1 ALIAS 'alias' COMMENT 'comment'");
    }

    @Test
    public void testCreateWithPath() {
        String fml = "create business_category c1.a.b comment 'comment'";
        CreateBusinessCategory createBusinessCategory = parse(fml, CreateBusinessCategory.class);
        assertEquals(createBusinessCategory.getIdentifier(), "a.b");
        assertEquals(createBusinessCategory.getBusinessUnit(), "c1");
    }

    @Test
    public void testSetComment() {
        String setComment = "ALTER business_category c1 SET COMMENT 'comment'";
        SetBusinessCategoryComment setBusinessCategoryComment = parse(setComment, SetBusinessCategoryComment.class);
        assertEquals(setBusinessCategoryComment.toString(), "ALTER BUSINESS_CATEGORY c1 SET COMMENT 'comment'");
    }

    @Test
    public void testDrop() {
        String dropBusinessCategoryComment = "DROP BUSINESS_CATEGORY c1";
        DropBusinessCategory dropBusinessCategory = parse(dropBusinessCategoryComment, DropBusinessCategory.class);
        assertEquals(dropBusinessCategory.toString(), "DROP BUSINESS_CATEGORY c1");
    }

    @Test
    public void testSetProperties() {
        String setProperties = "ALTER BUSINESS_CATEGORY a.b SET PROPERTIES('k'='b')";
        SetBusinessCategoryProperties setBusinessCategoryProperties = parse(setProperties,
            SetBusinessCategoryProperties.class);
        assertEquals(setBusinessCategoryProperties.toString(), "ALTER BUSINESS_CATEGORY a.b SET PROPERTIES ('k'='b')");
    }

    @Test
    public void testUnsetProperties() {
        String unSet = "ALTER BUSINESS_CATEGORY a.b UNSET PROPERTIES('a','b')";
        UnSetBusinessCategoryProperties unSetBusinessCategoryProperties = parse(unSet,
            UnSetBusinessCategoryProperties.class);
        assertEquals(unSetBusinessCategoryProperties.toString(),
            "ALTER BUSINESS_CATEGORY a.b UNSET PROPERTIES ('a','b')");
    }
}
