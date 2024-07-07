/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres.parser.util;

import com.aliyun.fastmodel.transform.hologres.client.property.HoloPropertyKey;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/4/16
 */
public class HologresPropertyUtilTest {

    @Test
    public void getPropertyValue() {
        String value = HologresPropertyUtil.getPropertyValue(HologresVersion.V1,
            HoloPropertyKey.DICTIONARY_ENCODING_COLUMN.getValue(), "c1:auto,c2");
        assertEquals("\"c1:auto,c2\"", value);

        value = HologresPropertyUtil.getPropertyValue(HologresVersion.V2,
            HoloPropertyKey.DICTIONARY_ENCODING_COLUMN.getValue(), "c1:auto,c2");
        assertEquals("\"c1\":auto,\"c2\":auto", value);
    }
}