package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import org.apache.hadoop.io.Text;

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by meepai on 2017/7/1.
 */
public final class RecordParser {

    /**
     * @param text
     * @return
     * @throws ParseException
     */
    public static List<String> parseRecord(Text text, DelimiterSet delimiters) throws ParseException {
        return parseRecord(text.toString(), delimiters);
    }

    /**
     * @param chars
     * @return
     * @throws ParseException
     */
    public static List<String> parseRecord(CharSequence chars, DelimiterSet delimiters) throws ParseException {
        return parseRecord(CharBuffer.wrap(chars), delimiters);
    }

    /**
     * @param buffer
     * @return
     * @throws ParseException
     */
    public static List<String> parseRecord(CharBuffer buffer, DelimiterSet delimiters) throws ParseException {
        char curChar = DelimiterSet.NULL_CHAR;
        ParseState state = ParseState.FIELD_START;
        int len = buffer.length();
        StringBuilder sb = null;

        List<String> outputs = new ArrayList<String>();
        char enclosingChar = delimiters.getEnclosedBy();
        char fieldDelim = delimiters.getFieldDelim();
        char recordDelim = delimiters.getRecordDelim();
        char escapeChar = delimiters.getEscapedBy();
        boolean enclosingRequired = delimiters.isEncloseRequired();
        for(int pos=0; pos<len; pos++){
            curChar = buffer.get();
            switch(state){
                case FIELD_START:
                    if(sb != null){
                        outputs.add(sb.toString());
                    }
                    sb = new StringBuilder();
                    if(enclosingChar == curChar){
                        state = ParseState.ENCLOSED_FIELD;
                    } else if(escapeChar == curChar){
                        state = ParseState.UNENCLOSED_ESCAPE;
                    } else if(fieldDelim == curChar){
                        continue;
                    } else if(recordDelim == curChar){
                        pos = len;
                    } else {
                        state = ParseState.UNENCLOSED_FIELD;
                        sb.append(curChar);
                        if(enclosingRequired){
                            throw new ParseException("Opening field-encloser expected at position " + pos);
                        }
                    }
                    break;
                case ENCLOSED_FIELD:
                    if(escapeChar == curChar){
                        state = ParseState.ENCLOSED_ESCAPE;
                    } else if(enclosingChar == curChar){
                        state = ParseState.ENCLOSED_EXPECT_DELIMITER;
                    } else {
                        sb.append(curChar);
                    }
                    break;
                case UNENCLOSED_FIELD:
                    if(escapeChar == curChar){
                        state = ParseState.UNENCLOSED_ESCAPE;
                    } else if(fieldDelim == curChar){
                        state = ParseState.FIELD_START;
                    } else if(recordDelim == curChar){
                        pos = len;
                    } else {
                        sb.append(curChar);
                    }
                    break;
                case ENCLOSED_ESCAPE:
                    sb.append(curChar);
                    state = ParseState.ENCLOSED_FIELD;
                    break;
                case ENCLOSED_EXPECT_DELIMITER:
                    if(fieldDelim == curChar){
                        state = ParseState.FIELD_START;
                    } else if(recordDelim == curChar){
                        pos = len;
                    } else {
                        throw new ParseException("Expected delimiter at position " + pos);
                    }
                    break;
                case UNENCLOSED_ESCAPE:
                    sb.append(curChar);
                    state = ParseState.UNENCLOSED_FIELD;
                    break;
                default:
                    throw new ParseException("Unexpected parser state: " + state);

            }
        }
        if(state == ParseState.FIELD_START && curChar == fieldDelim){
            if(sb != null){
                outputs.add(sb.toString());
                sb = new StringBuilder();
            }
        }
        if(sb != null){
            outputs.add(sb.toString());
        }
        return outputs;
    }


    public enum ParseState {
        FIELD_START,
        ENCLOSED_FIELD,
        UNENCLOSED_FIELD,
        ENCLOSED_ESCAPE,
        ENCLOSED_EXPECT_DELIMITER,
        UNENCLOSED_ESCAPE
    }

    public static void main(String[] args) throws ParseException {
        Text text = new Text("a,b,c");
        List<String> list = RecordParser.parseRecord(text, DelimiterSet.MYSQL_DELIMITERS);
        System.out.println(list.size());
        System.out.println(Arrays.toString(list.toArray()));
    }
}
