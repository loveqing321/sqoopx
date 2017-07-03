package com.deppon.hadoop.sqoopx.core.cli;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by meepai on 2017/6/19.
 */
public class ToolOptions implements Iterable<RelatedOptions> {

    private List<RelatedOptions> optionsGroups = new ArrayList<RelatedOptions>();

    private Set<String> optionsTitles = new HashSet<String>();

    /**
     * 添加options分组
     * @param options
     */
    public void addOptionsGroup(RelatedOptions options){
        if(optionsTitles.add(options.getTitle())){
            optionsGroups.add(options);
        }
    }

    /**
     * 合并已有分组
     */
    public RelatedOptions merge(){
        RelatedOptions merged = new RelatedOptions();
        for(Iterator<RelatedOptions> itr = iterator(); itr.hasNext();){
            Options options = itr.next();
            for(Iterator<Option> itr1 = options.getOptions().iterator(); itr1.hasNext();){
                merged.addOption(itr1.next());
            }
        }
        return merged;
    }

    /**
     * 迭代方法
     * @return
     */
    public Iterator<RelatedOptions> iterator() {
        return optionsGroups.iterator();
    }

    /**
     * 打印帮助信息
     */
    public void printHelp(){
        printHelp(new HelpFormatter());
    }

    /**
     * 打印帮助信息
     * @param helpFormatter
     */
    public void printHelp(HelpFormatter helpFormatter){
        printHelp(helpFormatter, new PrintWriter(System.out));
    }

    /**
     * 打印帮助信息，可指定输出源和输出格式
     * @param helpFormatter
     * @param pw
     */
    public void printHelp(HelpFormatter helpFormatter, PrintWriter pw){
        boolean first = true;
        for(Iterator<RelatedOptions> itr = iterator(); itr.hasNext(); first = false){
            RelatedOptions options = itr.next();
            if(!first) {
                pw.println("");
            }
            pw.println(options.getTitle());
            helpFormatter.printOptions(pw, helpFormatter.getWidth(), options, 0, 4);
        }
    }
}
