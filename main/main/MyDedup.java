package main;


import java.io.*;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

//Test file: D:\Desktop\CSCI4180\asg\asg1
class MyDedup{
    private static class MetaData implements Serializable{
        int numFile;
        String fingerPrintIndexPath;
        String chunkPath;

        public MetaData(int numFile, String fingerPrintIndexPath, String chunkPath) {
            this.numFile = numFile;
            this.fingerPrintIndexPath = fingerPrintIndexPath;
            this.chunkPath = chunkPath;
        }
    }
    //SHA1 fingerprint class: can generate fingerprint with static method
    private static class FingerPrint{
        byte[] sha1;
        static final private MessageDigest md;
        public FingerPrint(byte[] sha1) {
            this.sha1 = sha1;
        }

        static {
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        static FingerPrint SHA1(byte[] file,int offset,int len) throws DigestException {
            md.update(file,offset,len);
            return new FingerPrint(md.digest());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FingerPrint that = (FingerPrint) o;
            return Arrays.equals(sha1, that.sha1);
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(sha1);
        }
    }
    private static class Offset{
        int container;
        int offset;

        public Offset(int container, int offset) {
            this.container = container;
            this.offset = offset;
        }
    };
    //SHA1 -> ()
    private  static class FingerIndex implements Serializable{
        HashMap<FingerPrint,Offset> storage;
         FingerIndex fromFile(String filename){return null;};
         boolean exists(FingerPrint sha1){return false;};
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