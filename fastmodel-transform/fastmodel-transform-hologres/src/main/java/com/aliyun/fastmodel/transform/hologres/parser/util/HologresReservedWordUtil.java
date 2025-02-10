package com.aliyun.fastmodel.transform.hologres.parser.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.Vocabulary;

import static com.aliyun.fastmodel.transform.hologres.parser.HologreSQLLexer.VOCABULARY;

/**
 * hologres reserved keyword
 *
 * @author panguanjing
 * @date 2023/1/27
 */
public class HologresReservedWordUtil {

    private static final Pattern IDENTIFIER = Pattern.compile("'([A-Z_]+)'");

    private static final Set<String> SET = new HashSet<>();

    static {
        Vocabulary vocabulary = VOCABULARY;
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = Strings.nullToEmpty(vocabulary.getLiteralName(i));
            Matcher matcher = IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                SET.add(matcher.group(1));
            }
        }
        StringBuilder stringBuilder = new StringBuilder();
        try (InputStream is = HologresReservedWordUtil.class.getResourceAsStream("/hologres/builtin_function_name.txt")) {
            BufferedReader in = new BufferedReader(new InputStreamReader(is));
            String line = in.readLine();
            while (line != null) {
                stringBuilder.append(line);
                line = in.readLine();
            }
            Iterable<String> split = Splitter.on("|").trimResults().split(stringBuilder.toString());
            for (String t : split) {
                SET.remove(t);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static boolean isReservedKeyWord(String word) {
        return SET.contains(word.toUpperCase());
    }
}
