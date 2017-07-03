package com.deppon.hadoop.sqoopx.core.codegen;

import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import com.deppon.hadoop.sqoopx.core.jdbc.ConnManager;
import com.deppon.hadoop.sqoopx.core.mapreduce.*;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.IdentifierUtils;
import com.deppon.hadoop.sqoopx.core.util.SqlUtils;
import javassist.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by meepai on 2017/6/25.
 */
public class JdbcClassWriter extends BaseClassWriter {

    public static final String GENERATE_PACKAGE = Record.class.getPackage().getName() + ".generate";

    private SqoopxOptions options;

    private ConnManager connManager;

    private String className;

    private Map<String, Integer> columnTypes;

    private String[] columnNames;

    private String[] fields;

    private Map<String, String> fieldTypes = new HashMap<String, String>();

    public JdbcClassWriter(SqoopxOptions options, ConnManager connManager){
        this.options = options;
        this.connManager = connManager;
        this.className = this.options.getClassName();
        this.columnTypes = this.connManager.getColumnTypes();
        this.columnNames = getColumnNames(this.columnTypes);
        // pool的导入工作
        this.classPool.importPackage(RecordReadBridge.class.getPackage().getName());
        this.classPool.importPackage(StringBuilder.class.getPackage().getName());
        this.classPool.importPackage(List.class.getPackage().getName());
        this.classPool.importPackage(BooleanParser.class.getPackage().getName());
        this.classPool.importPackage(BytesWritable.class.getPackage().getName());
        this.classPool.importPackage(Text.class.getPackage().getName());
        this.classPool.importPackage(RecordParser.class.getPackage().getName());
    }

    public CtClass writeClassToPool() throws NotFoundException, IOException, CannotCompileException {
        CtClass recordClass = classPool.get(Record.class.getName());
        generate = classPool.makeClass(GENERATE_PACKAGE + "." + className, recordClass);
        // 生成字段
        this.generateFields(classPool, generate);
        this.generateConstructor(classPool, generate);
        this.generateLoadFromFieldsMethod(classPool, generate);
        this.generateParseMethod(classPool, generate);
        this.generateReadFields(classPool, generate);
        this.generateReadResultSet(classPool, generate);
        this.generateWriteOutput(classPool, generate);
        this.generateWritePsmt(classPool, generate);
        this.generateToString(classPool, generate);
        this.generateCompareTo(classPool, generate);
        return generate;
    }

    public void clearPool() {

    }

    /**
     * 根据数据库字段及名称，生成类属性
     * @param clazz
     */
    private void generateFields(ClassPool pool, CtClass clazz) throws IOException, NotFoundException, CannotCompileException {
        this.fields = new String[columnNames.length];
        for(int i=0; i<columnNames.length; i++){
            String col = columnNames[i];
            int sqlType = columnTypes.get(col);
            String javaType = SqlUtils.toJavaType(sqlType);
            if(javaType == null){
                throw new IOException("cannot resolve the type of this column [" + col + "]");
            }
            // 生成字段
            String fieldName = IdentifierUtils.toJavaIdentifier(col);
            this.fields[i] = fieldName;
            this.fieldTypes.put(fieldName, javaType.substring(javaType.lastIndexOf(".") + 1));
            CtField field = new CtField(pool.get(javaType), fieldName, clazz);
            field.setModifiers(Modifier.PRIVATE);
            clazz.addField(field);

            String setterName = "set" + StringUtils.capitalize(fieldName);
            clazz.addMethod(CtNewMethod.setter(setterName, field));
            String getterName = "get" + StringUtils.capitalize(fieldName);
            clazz.addMethod(CtNewMethod.getter(getterName, field));
        }

        // 生成一个__inputDelimiters对象
        CtField field = new CtField(pool.get(DelimiterSet.class.getName()), "__inputDelimiters", clazz);
        field.setModifiers(Modifier.PRIVATE | Modifier.FINAL);
        clazz.addField(field);
    }

    /**
     * @param pool
     * @param clazz
     * @throws IOException
     * @throws NotFoundException
     * @throws CannotCompileException
     */
    private void generateConstructor(ClassPool pool, CtClass clazz) throws IOException, NotFoundException, CannotCompileException {
        CtConstructor constructor = new CtConstructor(new CtClass[]{}, clazz);
        DelimiterSet delimiterSet = this.options.getOutputDelimiterSet();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("__inputDelimiters = new DelimiterSet((char)").append((int)delimiterSet.getFieldDelim());
        sb.append(", (char)").append((int)delimiterSet.getRecordDelim());
        sb.append(", (char)").append((int)delimiterSet.getEnclosedBy());
        sb.append(", (char)").append((int)delimiterSet.getEscapedBy());
        sb.append(", ").append(delimiterSet.isEncloseRequired()).append(");");
        sb.append("}");
        constructor.setBody(sb.toString());
        clazz.addConstructor(constructor);
    }

    /**
     * 根据具体类生成 parse方法
     * @param clazz
     */
    private void generateParseMethod(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        generateParseMethod(Text.class, pool, clazz);
    }

    /**
     * @param pcls
     * @param pool
     * @param clazz
     * @throws NotFoundException
     * @throws CannotCompileException
     */
    private void generateParseMethod(Class pcls, ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException{
        CtClass paramText = pool.get(pcls.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "parse", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        body.append("List __fields=RecordParser.parseRecord($1, __inputDelimiters);");
        body.append("$0.loadFromFields(__fields);");
        body.append("}");
        method.setBody(body.toString());
        // 设置异常
        CtClass ctClass = pool.get(ParseException.class.getName());
        method.setExceptionTypes(new CtClass[]{ctClass});
        clazz.addMethod(method);
    }

    /**
     * @param pool
     * @param clazz
     * @throws NotFoundException
     * @throws CannotCompileException
     */
    private void generateLoadFromFieldsMethod(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException{
        CtClass paramText = pool.get(List.class.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "loadFromFields", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        body.append("Iterator __it = $1.listIterator();");
        body.append("try{");
        body.append("String __cur_str=null;");
        fillFields(body);
        body.append("}catch(RuntimeException e){");
        body.append("throw new RuntimeException(\"Can't parse input data!\", e);}}");
        method.setBody(body.toString());
        clazz.addMethod(method);
    }

    /**
     * @param sb
     */
    private void fillFields(StringBuilder sb){
        for(int i=0; i<columnNames.length; i++){
            String col = columnNames[i];
            int sqlType = columnTypes.get(col);
            String javaType = SqlUtils.toJavaType(sqlType);
            // 生成字段
            String fieldName = IdentifierUtils.toJavaIdentifier(col);
            fillField(fieldName, javaType, sb);
        }
    }

    /**
     * @param field
     * @param javaType
     * @param sb
     */
    private void fillField(String field, String javaType, StringBuilder sb){
        sb.append("__cur_str=(String)__it.next();");
        parseNullValue(field, javaType, sb);
        if(javaType.equals("java.lang.String")){
            sb.append("$0.").append(field).append("=__cur_str;");
        } else if(javaType.equals("java.lang.Integer")){
            sb.append("$0.").append(field).append("=Integer.valueOf(__cur_str);");
        } else if(javaType.equals("java.lang.Long")){
            sb.append("$0.").append(field).append("=Long.valueOf(__cur_str);");
        } else if(javaType.equals("java.lang.Float")){
            sb.append("$0.").append(field).append("=Float.valueOf(__cur_str);");
        } else if(javaType.equals("java.lang.Double")){
            sb.append("$0.").append(field).append("=Double.valueOf(__cur_str);");
        } else if(javaType.equals("java.lang.Boolean")){
            sb.append("$0.").append(field).append("=BooleanParser.valueOf(__cur_str);");
        } else if(javaType.equals("java.sql.Time")){
            sb.append("$0.").append(field).append("=java.sql.Time.valueOf(__cur_str);");
        } else if(javaType.equals("java.sql.Timestamp")){
            sb.append("$0.").append(field).append("=java.sql.Timestamp.valueOf(__cur_str);");
        } else if(javaType.equals("java.math.BigDecimal")){
            sb.append("$0.").append(field).append("=new java.math.BigDecimal(__cur_str);");
        } else if(javaType.equals(BytesWritable.class.getName())){
            sb.append("String[] strByteVal=__cur_str.trim().split(\" \");");
            sb.append("byte[] byteVal=new byte[strByteVal.length;]");
            sb.append("for(int i=0; i<byteVal.length; i++){");
            sb.append("byteVal[i]=(byte)Integer.parseInt(strByteVal[i], 16);");
            sb.append("}");
            sb.append("$0.").append(field).append("=new BytesWritable(byteVal);");
        } else {
            // 其他的类型不做处理
        }
        sb.append("}");
    }

    /**
     * @param field
     * @param javaType
     * @param sb
     */
    private void parseNullValue(String field, String javaType, StringBuilder sb){
        if(javaType.equals("String")){
            sb.append("if(__cur_str.equals(\"").append(this.options.getInNullStringValue()).append("\")){");
            sb.append("$0.").append(field).append("=null;} else {");
        } else {
            sb.append("if(__cur_str.equals(\"").append(this.options.getInNullNonStringValue()).append("\")){");
            sb.append("$0.").append(field).append("=null;} else {");
        }
    }

    /**
     * 根据具体类生成 写入PreparedStatement方法
     * @param clazz
     */
    private void generateWritePsmt(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        CtClass paramText = pool.get(PreparedStatement.class.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "write", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        for(int i=0; i<columnNames.length; i++){
            body.append("RecordWriteBridge.write").append(fieldTypes.get(fields[i]));
            body.append("(").append(fields[i]).append(", ").append(i+1).append(", ");
            body.append(columnTypes.get(columnNames[i])).append(", ").append("$1);");
        }
        body.append("}");
        method.setBody(body.toString());
        // 设置异常
        CtClass ctClass = pool.get(SQLException.class.getName());
        method.setExceptionTypes(new CtClass[]{ctClass});
        clazz.addMethod(method);
    }

    /**
     * 根据具体类生成读取结果集方法
     * @param clazz
     */
    private void generateReadResultSet(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        CtClass paramText = pool.get(ResultSet.class.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "readFields", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        for(int i=0; i<columnNames.length; i++){
            body.append(fields[i]).append("=").append("RecordReadBridge.read").append(fieldTypes.get(fields[i]));
            body.append("(").append(i+1).append(", $1);");
        }
        body.append("}");
        method.setBody(body.toString());
        // 设置异常
        CtClass ctClass = pool.get(SQLException.class.getName());
        method.setExceptionTypes(new CtClass[]{ctClass});
        clazz.addMethod(method);
    }

    /**
     * 根据具体类生成写入DataOutputStream方法
     * @param clazz
     */
    private void generateWriteOutput(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        CtClass paramText = pool.get(DataOutput.class.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "write", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        for(int i=0; i<columnNames.length; i++){
            body.append("RecordWriteBridge.write").append(fieldTypes.get(fields[i]));
            body.append("(").append(fields[i]).append(", $1);");
        }
        body.append("}");
        method.setBody(body.toString());
        // 设置异常
        CtClass ctClass = pool.get(IOException.class.getName());
        method.setExceptionTypes(new CtClass[]{ctClass});
        clazz.addMethod(method);
    }

    /**
     * 根据具体类生成从DataInputStream读取字段方法
     * @param clazz
     */
    private void generateReadFields(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        CtClass paramText = pool.get(DataInput.class.getName());
        CtMethod method = new CtMethod(CtClass.voidType, "readFields", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        for(int i=0; i<columnNames.length; i++){
            body.append(fields[i]).append("=").append("RecordReadBridge.read").append(fieldTypes.get(fields[i]));
            body.append("($1);");
        }
        body.append("}");
        method.setBody(body.toString());
        // 设置异常
        CtClass ctClass = pool.get(IOException.class.getName());
        method.setExceptionTypes(new CtClass[]{ctClass});
        clazz.addMethod(method);
    }

    /**
     * 生成compare方法
     * @param clazz
     */
    private void generateCompareTo(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        String[] compareColumns = connManager.getPrimaryKeys();
        if(compareColumns == null || compareColumns.length == 0){
            compareColumns = columnNames;
        }
        CtClass paramText = pool.get(Object.class.getName());
        CtMethod method = new CtMethod(CtClass.intType, "compareTo", new CtClass[]{paramText}, clazz);
        StringBuilder body = new StringBuilder();
        body.append("{if($1==null){return 1;}");
        body.append("return ");
        for(int i=0; i<compareColumns.length; i++){
            if(i != 0){
                body.append("+");
            }
            body.append(compareColumns[i]).append(".compareTo(").append("((").append(clazz.getName()).append(")").append("$1).get");
            body.append(StringUtils.capitalize(compareColumns[i])).append("())");
        }
        body.append(";");
        body.append("}");
        method.setBody(body.toString());
        clazz.addMethod(method);
    }

    /**
     * 生成
     * @param pool
     * @param clazz
     */
    private void generateToString(ClassPool pool, CtClass clazz) throws NotFoundException, CannotCompileException {
        CtMethod method = new CtMethod(pool.get("java.lang.String"), "toString", new CtClass[0], clazz);
        StringBuilder body = new StringBuilder();
        body.append("{");
        body.append("StringBuilder sb = new StringBuilder();");
        for(int i=0; i<columnNames.length; i++){
            if(i != 0){
                body.append("sb.append(__inputDelimiters.getFieldDelim());");
            }
            String javaType = fieldTypes.get(fields[i]);
            String[] stringExpr = stringifierForType(javaType, columnNames[i]);
            body.append("if(").append(fields[i]).append("==null){");
            body.append("sb.append(");
            if(javaType.equals("String") && options.isHiveDropDelims()){
                body.append("FieldFormatter.hiveStringDropDelims(").append(stringExpr[0]).append(", __inputDelimiters));");
            } else if(javaType.equals("String") && options.getHiveDelimsReplacement() != null){
                body.append("FieldFormatter.hiveStringReplaceDelims(").append(stringExpr[0]).append(", ");
                body.append(options.getHiveDelimsReplacement()).append(", __inputDelimiters));");
            } else {
                body.append("FieldFormatter.escapeAndEnclose(").append(stringExpr[0]).append(", __inputDelimiters));");
            }
            body.append("} else {");
            body.append("sb.append(");
            if(javaType.equals("String") && options.isHiveDropDelims()){
                body.append("FieldFormatter.hiveStringDropDelims(").append(stringExpr[1]).append(", __inputDelimiters));");
            } else if(javaType.equals("String") && options.getHiveDelimsReplacement() != null){
                body.append("FieldFormatter.hiveStringReplaceDelims(").append(stringExpr[1]).append(", ");
                body.append(options.getHiveDelimsReplacement()).append(", __inputDelimiters));");
            } else {
                body.append("FieldFormatter.escapeAndEnclose(").append(stringExpr[1]).append(", __inputDelimiters));");
            }
            body.append("}");
        }
        body.append("sb.append(__inputDelimiters.getRecordDelim());");
        body.append("return sb.toString();");
        body.append("}");
        method.setBody(body.toString());
        clazz.addMethod(method);
    }

    /**
     * @param type
     * @param col
     * @return
     */
    private String[] stringifierForType(String type, String col){
        if(type.equals("String")){
            return new String[]{this.options.getNullStringValue(), col};
        } else if(type.equals("BigDecimal")){
            return new String[]{this.options.getNullStringValue(), col + ".toPlainString()"};
        } else {
            return new String[]{this.options.getNullNonStringValue(), "\"\" + " + col};
        }
    }

    /**
     * 根据columnTypes获取column名称
     * @return
     */
    private String[] getColumnNames(Map<String, Integer> columnTypes){
        if(columnTypes == null || columnTypes.size() == 0){
            return new String[0];
        }
        String[] colNames = this.options.getColumns();
        if(colNames == null){
            // 如果指定了表名，那么从数据库中获取所有列
            colNames = columnTypes.keySet().toArray(new String[0]);
        } else {
            // 如果指定了columns，那么启用指定的，但是需要对指定的columns分配字段类型
            String[] array = colNames;
            for(int i=0; i<array.length; i++){
                String col = array[i];
                for(Map.Entry<String, Integer> entry : columnTypes.entrySet()){
                    if(entry.getKey().equalsIgnoreCase(col) && !entry.getKey().equals(col)){
                        columnTypes.put(col, entry.getValue());
                    }
                }
            }
        }
        return colNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
