package com.deppon.hadoop.sqoopx.core;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by meepai on 2017/6/19.
 */
public class Test {

    public static void main(String[] args) throws IOException, URISyntaxException {

//        System.setProperty(ConfigurationConstants.LAUNCHER_JOB_ID, "123");
//        System.setProperty(ConfigurationConstants.LAUNCHER_CHILD_JOB_TAG, "tag1");
//
//        SqoopxMain.main(args);

//        System.out.println("a" + '\u0000' + "c");

//        FileInputStream fis = new FileInputStream("/Users/meepai/tmp/16.txt");
//        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//        String line;
//        while((line = br.readLine()) != null){
//            if(line.startsWith("1096af94-fe19-4e71-9150-eb5d446e5573")){
//                System.out.println(line);
//                System.out.println(line.charAt(line.length()-1));
//                String line2 = br.readLine();
//                System.out.println(line + line2);
//                break;
//            }
//        }
//        InputStreamReader isr = new InputStreamReader(fis);
//        StringBuilder sb = new StringBuilder();
//        getToStart(isr, sb);
//        getToEnd(isr, sb);
//
//        System.out.println(sb.toString());
        URL url = new URL("http", "localhost", 123, "/sfsf?a=2&b=3");
        URI uri = url.toURI();
        System.out.println(uri.getScheme());
        System.out.println(uri.getHost());
        System.out.println(uri.getAuthority());
        System.out.println(uri.getPath());
        System.out.println(uri.getQuery());
        System.out.println(uri.getFragment());
        System.out.println(url.toURI().toString());
    }

    public static void getToStart(InputStreamReader isr, StringBuilder sb) throws IOException {
        String str = "1096af94-fe19-4e71-9150-eb5d446e5573";
        char[] chars = new char[1];
        int idx = 0;
        while(isr.read(chars, 0, 1) != -1){
            if(chars[0] == str.charAt(idx)){
                idx ++;
                if(idx == str.length()){
                    sb.append("1096af94-fe19-4e71-9150-eb5d446e5573");
                    return;
                }
            } else {
                idx = 0;
            }
        }
        throw new IOException("");
    }

    public static void getToEnd(InputStreamReader isr, StringBuilder sb) throws IOException {
        String str = "1096af94-fe19-4e71-9150-eb5d446e5573";
        char[] chars = new char[1];
        int idx = 0;
        while(isr.read(chars, 0, 1) != -1){
            sb.append(chars[0]);
            System.out.println(sb.toString());
            if(chars[0] == str.charAt(idx)){
                idx ++;
                if(idx == str.length()){
                    return;
                }
            } else {
                idx = 0;
            }
        }
        throw new IOException("");
    }
}
