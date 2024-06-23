package com.aliyun.fastmodel.transform.doris.parser.visitor;

import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisGenericDataType;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public interface DorisAstVisitor<R, C> extends ExtensionAstVisitor<R, C> {
    default R visitDorisGenericDataType(DorisGenericDataType dorisGenericDataType, C context) {
        return visitGenericDataType(dorisGenericDataType, context);
    }
}
