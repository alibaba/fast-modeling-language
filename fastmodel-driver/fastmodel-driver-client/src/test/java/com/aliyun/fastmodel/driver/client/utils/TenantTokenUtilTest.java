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

package com.aliyun.fastmodel.driver.client.utils;

import com.aliyun.fastmodel.driver.client.command.tenant.TenantTokenUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/16
 */
public class TenantTokenUtilTest {

    @Test
    public void getSignature() {
        String token = TenantTokenUtil.getSignature("token", "a=1&b=2&c=1");
        assertEquals("56d14af6bdf0af187152b09cd0d56eb3c61d2e36c74db3a0ca215c1f4fef6579", token);
    }

    @Test
    public void testSignature() {
        String signature = TenantTokenUtil.getSignature(
            "96063edcb9f5d80926d14f70e491ff1b73463f70ddc2773a843bc98e4e4ca36f",
            "                                                                            "
                + "query=show+tables+like+%27%E5%AE%B6%E5%A8%83%27&database=1_14255&engine=&baseKey=base_dp&token"
                + "=96063edcb9f5d80926d14f70e491ff1b73463f70ddc2773a843bc98e4e4ca36f&timestamp=1614593300617");
        assertEquals("4a15e628ce7aa2c2d67f168bbb443fe400499a754e617598f55f995936d5f0e1", signature);
    }

    @Test
    public void testSignWithDecode() {
        String signature = TenantTokenUtil.getSignature(
            "96063edcb9f5d80926d14f70e491ff1b73463f70ddc2773a843bc98e4e4ca36f",
            "query=show+tables+like+%27%E4%B8%AD%E6%96%87%27&database=1_14255&engine=&baseKey=base_dp&token"
                + "=96063edcb9f5d80926d14f70e491ff1b73463f70ddc2773a843bc98e4e4ca36f&timestamp=1614600956510");
        assertEquals(signature, "1d570ca5ac0256dc459ad25e472b1ce283c904303f750d36e334a859e1c37a59");
    }

}