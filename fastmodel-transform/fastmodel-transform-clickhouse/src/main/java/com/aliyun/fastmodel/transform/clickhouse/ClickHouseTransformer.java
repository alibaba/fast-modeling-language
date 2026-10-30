/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.clickhouse.client.converter.ClickHouseClientConverter;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
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

    private final ClickHouseClientConverter clickHouseClientConverter;

    public ClickHouseTransformer() {
        clickHouseLanguageParser = new ClickHouseLanguageParser();
        clickHouseClientConverter = new ClickHouseClientConverter();
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.CLICKHOUSE, IVersion.getDefault());
        ClickHouseContext clickHouseContext = new ClickHouseContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, clickHouseContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, clickHouseContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)clickHouseLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return clickHouseClientConverter.convertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        ClickHouseContext clickHouseContext = new ClickHouseContext(context);
        return clickHouseClientConverter.convertToTable(table, clickHouseContext);
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return clickHouseClientConverter.getPropertyConverter().create(name, value);
    }
}
