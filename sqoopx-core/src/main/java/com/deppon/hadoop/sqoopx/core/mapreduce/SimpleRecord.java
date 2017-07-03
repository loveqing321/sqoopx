package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by meepai on 2017/6/21.
 */
public class SimpleRecord extends Record {

    private static final Logger log = Logger.getLogger(SimpleRecord.class);

    public String word;

    public int count;

    public SimpleRecord(){

    }

    public void parse(Text text) throws ParseException {

    }

    public void write(PreparedStatement psmt) throws SQLException {

    }

    public void readFields(ResultSet resultSet) throws SQLException {
        System.out.println("readFields: " + resultSet.getObject(1));
        word = resultSet.getString(1);
        count = resultSet.getInt(2);
    }

    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, word);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word = Text.readString(dataInput);
        count = dataInput.readInt();
    }

    public int compareTo(Record o) {
        if(o == null){
            return 1;
        }
        return word.compareTo(((SimpleRecord)o).word);
    }

    @Override
    public String toString() {
        return word + ":" + count;
    }
}
