package cn.com.zhangd.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FileTools {
    /***
     *
     * @param strOldname
     *            原始文件名
     * @param strNewname
     *            目的文件名
     */
    public void renameFile(String strOldname, String strNewname) {
        File fileOld = new File(strOldname);
        File fileNew = new File(strNewname);
        if (fileOld.exists() && !fileNew.exists()) {
            fileOld.renameTo(fileNew);
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
        if (srcFile.exists() && !destFile.exists()) {
            srcFile.renameTo(destFile);
        }
    }

    /***
     * 删除目录与目录下的所有文件
     *
     * @param dir
     * @return
     */
    public boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                // 递归删除目录中的子目录下
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    /***
     * 删除文件，不含目录
     *
     * @param strFile
     */
    public void deleteFile(String strFile) {
        File file = new File(strFile);
        if (file.exists()) {
            file.delete();
        }
    }

    /***
     * 读取文件内容，并返回内容列表
     * @param fileName
     * @return
     */
    public List<String> readFileByLines(String fileName) {
        List<String> contents = new ArrayList<String>();

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                contents.add(tempString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return contents;
    }

    /***
     * 读取文件内容，并返回内容列表
     * @param fileName
     * @return
     */
    public long readFileLines(String fileName) {
        long returnl = 0;
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                returnl++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return returnl;
    }

    /***
     * 读取文件中的前几行，并返回内容列表
     * @param fileName
     * @return
     */
    public List<String> readFileByLinesNumber(String fileName, int lineNumber) {
        List<String> contents = new ArrayList<String>();

        int i = 1;
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                if (lineNumber == i) {
                    break;
                } else {
                    contents.add(tempString);
                }
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return contents;
    }


    /***
     * 读取文件中的前几行，并返回内容列表
     * @param fileName
     * @return
     */
    public List<String> readFileByLinesBeginToEnd(String fileName, long beginNumber, int limitSize) {
        Vector<String> vector = new Vector<String>(limitSize);
        long lineNumber = 0;
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                lineNumber++;
                if (lineNumber == beginNumber + limitSize) {
                    break;
                } else if (beginNumber <= lineNumber) {
                    vector.add(tempString);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return vector;
    }

    /***
     * 将内容追加到文件中
     *
     * @param fileName
     * @param listContent
     */
    public void appendContentToFile(String fileName, List<String> listContent) {
        FileWriter writer = null;
        try {
            writer = new FileWriter(fileName, true);
            for (String content : listContent) {
                writer.write(content + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /***
     * 将内容写入到文件中。
     *
     * @param file
     * @param content
     */
    public boolean writeToTxt(File file, String content) {
        boolean bfalg = false;
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);
            writer.write(content);
            writer.newLine();// 换行
            writer.flush();
            bfalg = true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bfalg;
    }
}
