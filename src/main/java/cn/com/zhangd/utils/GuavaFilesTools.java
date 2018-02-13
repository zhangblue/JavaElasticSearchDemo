package cn.com.zhangd.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhangdi on 2017/04/12.
 * google guava Files对象函数
 */
public class GuavaFilesTools {

    /***
     * 复制文件
     * @param srcPath 源文件目录
     * @param targetPath 目标文件目录
     * @return
     */
    public void copyFile(String srcPath, String targetPath) {
        try {
            Files.copy(new File(srcPath), new File(targetPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * 移动文件
     * @param srcPath 源文件目录
     * @param targetPath 目标文件目录
     */
    public void moveFile(String srcPath, String targetPath) {
        try {
            Files.move(new File(srcPath), new File(targetPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     *
     * @param srcFile
     *            原始文件名
     * @param destFile
     *            目的文件名
     */
    public void renameFile(File srcFile, File destFile) {
        try {
            Files.move(srcFile, destFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * 遍历文件夹下的所有文件
     *
     * @param strPath 文件夹目录
     * @return
     */
    public Iterator<File> getFileTraversal(String strPath) {
        Iterable<File> it = Files.fileTreeTraverser().breadthFirstTraversal(new File(strPath));
        return it.iterator();
    }

    /***
     * 读取小文件的内容
     * @param strPath
     * @return
     */
    public List<String> readSmallFile(String strPath) {
        List<String> list = new ArrayList<String>();
        try {
            list = Files.readLines(new File(strPath), Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 读取大文件内容
     *
     * @param strPath      文件路径
     * @param beginLineNum 读取开始行号
     * @param readSize     每次读取条数
     * @return
     */
    public LargeLineProcessor readLargeFile(String strPath, long beginLineNum, int readSize) {
        LargeLineProcessor largeLineProcessor = new LargeLineProcessor(beginLineNum, readSize);

        try {
            Files.readLines(new File(strPath), Charsets.UTF_8, largeLineProcessor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return largeLineProcessor;
    }
}
