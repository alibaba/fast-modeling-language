package com.aliyun.fastmodel.transform.postgresql.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/13
 */
@AutoService(LanguageParser.class)
public class PostgreSQLLanguageParser implements LanguageParser<Node, ReverseContext> {
    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, PostgreSQLParser::root);
    }

    private Node getNode(String text, ReverseContext context, Function<PostgreSQLParser, ParserRuleContext> functionalInterface) {
        ParserRuleContext tree = ParserHelper.getNode(
            text,
            PostgreSQLLexer::new,
            PostgreSQLParser::new,
            parser -> {
                PostgreSQLParser starRocksParser = (PostgreSQLParser)parser;
                return functionalInterface.apply(starRocksParser);
            }
        );
        return tree.accept(new PostgreSQLAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(code, context, PostgreSQLParser::b_expr);
    }
}
