package cn.com.zhangd.utils;

import com.google.common.io.LineProcessor;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by zhangdi on 2017/04/12.
 * <p>
 * 分段读取大文件接口实现
 */
public class LargeLineProcessor implements LineProcessor<Vector> {

    private long beginLine = 0;//从第几行开始读
    private long nowLineNumber = 0;//当前读到的行号
    private int readSize = 0;//每次读取条数
    private Vector<String> listContent = null;

    public long getNowLineNumber() {
        return nowLineNumber;
    }

    public LargeLineProcessor(long beginLine, int readSize) {
        this.beginLine = beginLine;
        this.readSize = readSize;
        listContent = new Vector(readSize);
    }

    @Override
    public boolean processLine(String line) throws IOException {
        nowLineNumber++;
        if (beginLine <= nowLineNumber) {
            listContent.add(line);
        }

        if (listContent.size() == readSize) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Vector<String> getResult() {
        return listContent;
    }
}
