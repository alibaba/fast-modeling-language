package com.aliyun.fastmodel.transform.starrocks.parser.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLexer;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.Vocabulary;

/**
 * hologres reserved keyword
 *
 * @author panguanjing
 * @date 2023/11/6
 */
public class StarRocksReservedWordUtil {

    private static final Pattern IDENTIFIER = Pattern.compile("'([A-Z_]+)'");

    private static final Set<String> SET = new HashSet<>();

    static {
        Vocabulary vocabulary = StarRocksLexer.VOCABULARY;
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getLiteralName(i));
            Matcher matcher = IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                SET.add(matcher.group(1));
            }
        }
    }

    public static boolean isReservedKeyWord(String word) {
        return SET.contains(word.toUpperCase());
    }
}
