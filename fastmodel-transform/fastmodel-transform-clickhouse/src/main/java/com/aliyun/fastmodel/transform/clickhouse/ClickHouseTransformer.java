/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseLanguageParser;
import com.google.auto.service.AutoService;

/**
 * clickHouse transformer
 *
 * @author panguanjing
 * @date 2022/7/9
 */
@AutoService(Transformer.class)
@Dialect(Constants.CLICKHOUSE)
public class ClickHouseTransformer implements Transformer<BaseStatement> {

    private final ClickHouseLanguageParser clickHouseLanguageParser;

    public ClickHouseTransformer() {
        clickHouseLanguageParser = new ClickHouseLanguageParser();
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)clickHouseLanguageParser.parseNode(dialectNode.getNode(), context);
    }
}
