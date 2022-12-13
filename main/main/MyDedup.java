package main;


import java.io.*;
import java.nio.ByteBuffer;
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
        //container
        int container;
        //offset from container start
        int offset;

        public Offset(int container, int offset) {
            this.container = container;
            this.offset = offset;
        }
    };
    private  static class FingerIndex implements Serializable{
        HashMap<FingerPrint,Offset> storage;
         FingerIndex fromFile(String filename) throws IOException, ClassNotFoundException {
             ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
             FingerIndex result= (FingerIndex) io.readObject();
             io.close();
             return result;
         };
         void toFile(String filename) throws IOException {
             ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(filename));
             io.writeObject(this);
             io.close();
         }

        public FingerIndex() {
            storage=new HashMap<>();
        }
        //get the fingerprint of the chunk. return null if the chunk does not exist
        Offset get(FingerPrint sha1){
             return storage.get(sha1);
         };
         Offset put(FingerPrint pf,Offset offset){
             return storage.put(pf,offset);
         };
    }
    private static class ChunkFile implements Serializable{
        //buffer
        private ByteBuffer bf_in;
        private ByteBuffer bf_out;
        //the final bit of each container
        private ArrayList<Integer> containerEndLoc;
        private BufferedInputStream in;
        private BufferedOutputStream out;
        //Put the chunk into the buffer. If buffer is full, flush it. return the container number
        //and the offset from the start of the file
        private Offset appendChunk(byte[] file,int offset,int len){return null;};
        //Return the chunk at offset. Cache the container
        private byte[] readChunk(Offset offset){
            return null;
        }
        ChunkFile fromFile(String filename) throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
            ChunkFile result= (ChunkFile) io.readObject();
            io.close();
            return result;
        };
        void toFile(String filename) throws IOException {
            ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(filename));
            io.writeObject(this);
            io.close();
        }
    }
    private static class FileRecipe implements Serializable{
        private ArrayList<Offset> chunks;
        private String filename;
        FileRecipe fromFile(String filename) throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
            FileRecipe result= (FileRecipe) io.readObject();
            io.close();
            return result;
        };
        void toFile(String filename) throws IOException {
            ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(filename));
            io.writeObject(this);
            io.close();
        }

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

        }


    }
}