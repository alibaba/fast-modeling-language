/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.example.integrate;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;

/**
 * MaxCompute to hologres integrate
 *
 * @author panguanjing
 * @date 2022/6/17
 */
public class MaxComputeToHologresIntegrate {

    /**
     * mc to hologres
     *
     * @param dialectNode
     * @return {@link DialectNode}
     */
    public DialectNode mcToHologres(DialectNode dialectNode) {
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());
        BaseStatement baseStatement = (BaseStatement)transformer.reverse(dialectNode);
        Transformer hologresTransform = TransformerFactory.getInstance().get(DialectMeta.getHologres());
        return hologresTransform.transform(baseStatement, HologresTransformContext.builder().build());
    }
}
