package com.aliyun.fastmodel.transform.postgresql.parser.visitor;

import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLArrayDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLGenericDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLIntervalDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.expr.WithDataTypeNameExpression;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/16
 */
public interface PostgreSQLVisitor<R, C> extends ExtensionAstVisitor<R, C> {
    /**
     * visit array bounds
     *
     * @param arrayBounds
     * @param context
     * @return
     */
    default R visitArrayBounds(ArrayBounds arrayBounds, C context) {
        return visitNode(arrayBounds, context);
    }

    /**
     * visit begin work
     *
     * @param beginWork
     * @param context
     * @return
     */
    default R visitBeginWork(BeginWork beginWork, C context) {
        return visitStatement(beginWork, context);
    }

    /**
     * visit commit work
     *
     * @param commitWork
     * @param context
     * @return
     */
    default R visitCommitWork(CommitWork commitWork, C context) {
        return visitStatement(commitWork, context);
    }

    /**
     * visitPostgreSQLArrayDataType
     *
     * @param postgreSQLArrayDataType
     * @param context
     * @return
     */
    default R visitPostgreSQLArrayDataType(PostgreSQLArrayDataType postgreSQLArrayDataType, C context) {
        return visitExpression(postgreSQLArrayDataType, context);
    }

    /**
     * visitWithDataTypeNameExpression
     *
     * @param withDataTypeNameExpression
     * @param context
     * @return
     */
    default R visitWithDataTypeNameExpression(WithDataTypeNameExpression withDataTypeNameExpression, C context) {
        return visitExpression(withDataTypeNameExpression, context);
    }

    /**
     * visitPostgreSQLGenericDataType
     *
     * @param postgreSQLGenericDataType
     * @param context
     * @return
     */
    default R visitPostgreSQLGenericDataType(PostgreSQLGenericDataType postgreSQLGenericDataType, C context) {
        return visitGenericDataType(postgreSQLGenericDataType, context);
    }

    /**
     * visit postgreSQLIntervalDataType
     *
     * @param postgreSQLIntervalDataType
     * @param context
     * @return
     */
    default R visitPostgreSQLIntervalDataType(PostgreSQLIntervalDataType postgreSQLIntervalDataType, C context) {
        return visitDataType(postgreSQLIntervalDataType, context);
    }
}
