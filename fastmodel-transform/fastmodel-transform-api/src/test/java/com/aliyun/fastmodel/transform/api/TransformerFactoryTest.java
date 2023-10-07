/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public class TransformerFactoryTest {

    @Test
    public void testTransform() {
        //init factory
        TransformerFactory factory = TransformerFactory.getInstance();
        //set engineMeta
        DialectMeta dialectMeta = DialectMeta.getMaxCompute();
        Transformer<BaseStatement> transformer = factory.get(dialectMeta);
        assertNull(transformer);
    }
}
