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

package com.aliyun.fastmodel.core.shared.constants;

import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/14
 */
public class TableDetailTypeTest {

    @Test
    public void testGetByCode() throws Exception {
        TableDetailType result = TableDetailType.getByCode("normal", TableType.DIM);
        assertEquals(TableDetailType.NORMAL_DIM, result);
        result = TableDetailType.getByCode("advanced", TableType.DWS);
        assertEquals(TableDetailType.ADVANCED_DWS, result);
    }

    @Test
    public void testGetByComment() {
        TableDetailType r
            = TableDetailType.getByComment("普通维度表");
        assertEquals(r.getComment(), "普通维度表");
    }

    @Test
    public void testOds() {
        TableDetailType tableDetailType = TableDetailType.getByCode("ods", TableType.ODS);
        assertEquals(TableDetailType.ODS, tableDetailType);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev
// .com/forum#!/testme