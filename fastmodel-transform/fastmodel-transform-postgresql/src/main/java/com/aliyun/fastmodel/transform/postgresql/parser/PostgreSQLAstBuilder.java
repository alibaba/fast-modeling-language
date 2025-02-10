package com.aliyun.fastmodel.transform.postgresql.parser;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/13
 */
public class PostgreSQLAstBuilder extends PostgreSQLParserBaseVisitor<Node> {
    private final ReverseContext reverseContext;

    public PostgreSQLAstBuilder(ReverseContext context) {
        this.reverseContext = context;
    }
}
