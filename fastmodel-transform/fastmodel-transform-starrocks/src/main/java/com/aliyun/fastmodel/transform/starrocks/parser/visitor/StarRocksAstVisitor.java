package com.aliyun.fastmodel.transform.starrocks.parser.visitor;

import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;

/**
 * star rocks visitor
 *
 * @author panguanjing
 * @date 2023/9/12
 */
public interface StarRocksAstVisitor<R, C> extends ExtensionAstVisitor<R, C> {

    /**
     * visit starRocks GenericDataType
     *
     * @param starRocksGenericDataType genericDataType
     * @param context                  context
     * @return R
     */
    default R visitStarRocksGenericDataType(StarRocksGenericDataType starRocksGenericDataType, C context) {
        return visitGenericDataType(starRocksGenericDataType, context);
    }

}
