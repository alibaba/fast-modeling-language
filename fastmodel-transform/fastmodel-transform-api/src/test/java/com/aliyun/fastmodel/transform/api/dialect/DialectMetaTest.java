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

package com.aliyun.fastmodel.transform.api.dialect;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/20
 */
public class DialectMetaTest {

    @Test
    public void testGetByName() {
        DialectMeta hive = DialectMeta.getByNameAndVersion("hive", IVersion.getDefault());
        assertNotNull(hive);
    }

    @Test
    public void testGetByNameMc() {
        DialectMeta dialectMeta = DialectMeta.getByNameAndVersion("maxcompute", IVersion.getDefault());
        assertNotNull(dialectMeta);
    }

    @Test
    public void testGetByNameHologres() {
        DialectMeta dialectMeta = DialectMeta.getByNameAndVersion("hologres", IVersion.getDefault());
        assertNotNull(dialectMeta);
    }

}