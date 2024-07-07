/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.generator;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.BaseGeneratorRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.base.Preconditions;

/**
 * DefaultCodeGenerator
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class DefaultCodeGenerator implements CodeGenerator {

    @Override
    public DdlGeneratorResult generate(DdlGeneratorModelRequest request) {
        return generate(request, (t, r) -> {
            DdlGeneratorModelRequest ddlGeneratorSqlRequest = (DdlGeneratorModelRequest)r;
            if (ddlGeneratorSqlRequest.getBefore() != null) {
                return t.reverseTable(ddlGeneratorSqlRequest.getBefore(), ReverseContext.builder().build());
            }
            return null;
        });
    }

    @Override
    public DdlGeneratorResult generate(DdlGeneratorSqlRequest request) {
        return generate(request, (t, r) -> {
            DdlGeneratorSqlRequest ddlGeneratorSqlRequest = (DdlGeneratorSqlRequest)r;
            if (ddlGeneratorSqlRequest.getBefore() != null) {
                return t.reverse(new DialectNode(ddlGeneratorSqlRequest.getBefore()));
            }
            return null;
        });
    }

    /**
     * common generate method
     *
     * @param request
     * @param function
     * @return {@link DdlGeneratorResult}
     */
    private DdlGeneratorResult generate(BaseGeneratorRequest request, BiFunction<Transformer, BaseGeneratorRequest, Node> function) {
        //参数校验
        Preconditions.checkNotNull(request, "request can't be null");
        Preconditions.checkNotNull(request.getAfter(), "table can't be null");
        Preconditions.checkNotNull(request.getConfig(), "config can't be null");
        //根据function提供的结果，获取ddl内容和reverse context
        Table tableClientDTO = request.getAfter();
        DialectMeta dialectMeta = request.getConfig().getDialectMeta();
        Transformer<Node> transformer = TransformerFactory.getInstance().get(dialectMeta);
        Preconditions.checkNotNull(transformer, "can't find the transformer with dialectMeta:" + dialectMeta);
        //将传入的request转为node
        Node reverse = function.apply(transformer, request);
        Node node = transformer.reverseTable(tableClientDTO, ReverseContext.builder()
            .version(request.getConfig().getDialectMeta().getVersion())
            .build());
        //比较node
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare((BaseStatement)reverse, (BaseStatement)node,
            request.getConfig().isDropIfExist() ? CompareStrategy.FULL : CompareStrategy.INCREMENTAL);
        //将返回的node，转为目标的ddl语句
        List<DialectNode> dialectNodes = compare.stream().map(
            statement -> {
                TransformContext build = TransformContext.builder().database(request.getAfter().getDatabase())
                    .appendSemicolon(request.getConfig().isAppendSemicolon())
                    .schema(request.getAfter().getSchema()).build();
                return transformer.transform(statement,
                    build);
            }).collect(Collectors.toList());
        return DdlGeneratorResult.builder().dialectNodes(dialectNodes).build();
    }

    @Override
    public DdlTableResult reverse(DdlReverseSqlRequest request) {
        Preconditions.checkNotNull(request, "request can't be null");
        DialectMeta dialectMeta = request.getDialectMeta();
        Preconditions.checkNotNull(dialectMeta, "dialect can't be null");
        Transformer transformer = TransformerFactory.getInstance().get(dialectMeta);
        Preconditions.checkNotNull(transformer, "can't find the transformer with dialectMeta:" + dialectMeta);
        String code = request.getCode();
        Node reverse = null;
        reverse = transformer.reverse(new DialectNode(code), ReverseContext.builder().merge(true)
            .version(dialectMeta.getVersion())
            .build());
        Preconditions.checkNotNull(reverse, "reverse is null with the code");
        Table table = transformer.transformTable(reverse,
            TransformContext.builder()
                .database(request.getDatabase())
                .schema(request.getSchema())
                .appendSemicolon(true)
                .build());
        return new DdlTableResult(table);
    }

}
