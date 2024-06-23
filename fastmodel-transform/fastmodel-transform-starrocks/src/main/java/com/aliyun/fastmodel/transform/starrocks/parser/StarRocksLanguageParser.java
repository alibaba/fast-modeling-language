package com.aliyun.fastmodel.transform.starrocks.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.ParserRuleContext;

/**
 * StarRocksLanguageParser
 *
 * @author panguanjing
 * @date 2023/9/5
 */
@AutoService(LanguageParser.class)
public class StarRocksLanguageParser implements LanguageParser<Node, ReverseContext> {

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, StarRocksParser::sqlStatements);
    }

    private Node getNode(String text, ReverseContext context, Function<StarRocksParser, ParserRuleContext> functionalInterface) {
        ParserRuleContext tree = ParserHelper.getNode(
            text,
            charStream -> new StarRocksLexer(charStream),
            tokenStream -> new StarRocksParser(tokenStream),
            parser -> {
                StarRocksParser starRocksParser = (StarRocksParser)parser;
                return functionalInterface.apply(starRocksParser);
            }
        );
        return tree.accept(new StarRocksAstBuilder(context));
    }

    @Override
    public BaseDataType parseDataType(String code, ReverseContext context) throws ParseException {
        return (BaseDataType)getNode(code, context, StarRocksParser::type);
    }
}
