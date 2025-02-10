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

package com.aliyun.fastmodel.transform.hologres.client.property;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/9
 */
public class HologresPropertyKeyTest {

    @Test
    public void getByValue() {
        HologresPropertyKey byValue = HologresPropertyKey.getByValue("refresh_guc_hg_enable_start_auto_analyze_worker");
        assertTrue(byValue.isSupportPrint());
    }
}