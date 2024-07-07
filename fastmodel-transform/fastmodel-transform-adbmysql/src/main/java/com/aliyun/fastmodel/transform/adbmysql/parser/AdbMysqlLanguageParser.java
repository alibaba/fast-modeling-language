package com.aliyun.fastmodel.transform.adbmysql.parser;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * AdbMysqlLanguageParser
 *
 * @author panguanjing
 * @date 2023/2/10
 */
@AutoService(LanguageParser.class)
public class AdbMysqlLanguageParser implements LanguageParser<Node, ReverseContext> {

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, AdbMysqlLexer::new,
            AdbMysqlParser::new,
            parser -> {
                AdbMysqlParser mysqlParser = (AdbMysqlParser)parser;
                return mysqlParser.root();
            }
        );
        return parserRuleContext.accept(new AdbMysqlAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String text, ReverseContext context) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, AdbMysqlLexer::new,
            AdbMysqlParser::new,
            parser -> {
                AdbMysqlParser mysqlParser = (AdbMysqlParser)parser;
                return mysqlParser.dataType();
            }
        );
        return (BaseDataType)parserRuleContext.accept(new AdbMysqlAstBuilder(context));
    }

    @Override
    public <T> T parseExpression(String text) throws ParseException {
        ParserRuleContext parserRuleContext = ParserHelper.getNode(text, AdbMysqlLexer::new,
            AdbMysqlParser::new,
            parser -> {
                AdbMysqlParser mysqlParser = (AdbMysqlParser)parser;
                return mysqlParser.expression();
            }
        );
        return (T)parserRuleContext.accept(new AdbMysqlAstBuilder(ReverseContext.builder().build()));
    }
}


