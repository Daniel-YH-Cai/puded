package main;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

//Test file: D:\Desktop\CSCI4180\asg\asg1
class MyDedup{
    private static class MetaData implements Serializable{

    }
    private static class ByteString{

    }
    private static class Offset{
        int container;
        int offset;
    };
    //SHA1 -> ()
    private  static class FingerIndex implements Serializable{
        HashMap<ByteString,Offset> storage;
         FingerIndex fromFile(String filename){return null;};
         boolean exists(ByteString sha1){return false;};
         boolean put(byte[] buff,int start,int end){return false;};
    }
    private static class ChunkFile{
        //buffer
        private boolean appendFile(){return true;};
    }
    private static class FileRecipe{
        private ArrayList<Offset> chunks;
        private String filePath;
    }
    private static byte[] readFileBytes(File f) throws IOException {
        int fileSize=(int)f.length();
        BufferedInputStream inputStream=new BufferedInputStream(new FileInputStream(f));
        byte[] buffer=new byte[fileSize];
        int start=0;
        int numRead=0;
        while ((numRead=inputStream.read(buffer,start,fileSize-start))!=0){
            start=start+numRead;
        }
        return buffer;
    }

    public  static void main(String[] args) throws IOException {
        File f=new File(args[1]);
        byte[] fileBytes=readFileBytes(f);
        for(int i=0;i<fileBytes.length;i++){
            //if an anchor is hit
            //
        }


    }
}