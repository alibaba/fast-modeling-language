package com.aliyun.fastmodel.transform.spark.parser;

import java.util.function.Function;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.common.parser.lexer.CaseChangingCharStream;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.spark.format.SparkAstBuilder;
import com.google.auto.service.AutoService;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;

/**
 * SparkLanguageParser
 *
 * @author panguanjing
 * @date 2023/2/18
 */
@AutoService(LanguageParser.class)
public class SparkLanguageParser implements LanguageParser<Node, ReverseContext> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    @Override
    public Node parseNode(String text, ReverseContext context) throws ParseException {
        return getNode(text, context, SparkParser::singleStatement);
    }

    private Node getNode(String text, ReverseContext context, Function<SparkParser, ParserRuleContext> functionalInterface) {
        String code = StripUtils.appendSemicolon(text);
        CodePointCharStream charStream = CharStreams.fromString(code);
        CaseChangingCharStream caseChangingCharStream = new CaseChangingCharStream(charStream, true);
        SparkLexer lexer = new SparkLexer(caseChangingCharStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(LISTENER);
        CommonTokenStream commonTokenStream = new CommonTokenStream(lexer);
        SparkParser sparkParser = new SparkParser(commonTokenStream);
        sparkParser.removeErrorListeners();
        sparkParser.addErrorListener(LISTENER);
        ParserRuleContext tree;
        try {
            sparkParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = functionalInterface.apply(sparkParser);
        } catch (Throwable e) {
            commonTokenStream.seek(0);
            sparkParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = functionalInterface.apply(sparkParser);
        }
        return tree.accept(new SparkAstBuilder(context));
    }
}
