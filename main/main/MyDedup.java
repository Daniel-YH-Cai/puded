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
        int len;

        public Offset(int container, int offset,int len) {
            this.container = container;
            this.offset = offset;
            this.len=len;
        }
    };
    private  static class FingerIndex implements Serializable{
        HashMap<FingerPrint,Offset> storage;
         private static FingerIndex fromFile(String filename) throws IOException, ClassNotFoundException {
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
        private static final String ChunkFileName="";
        //buffer
        transient private ByteBuffer bf_in;
        transient private ByteBuffer bf_out;
        //the final bit of each container
        private ArrayList<Integer> containerEndLoc;
        transient private BufferedInputStream in;
        transient private BufferedOutputStream out;
        //Put the chunk into the buffer. If buffer is full, flush it. return the container number
        //and the offset from the start of the file
        private Offset appendChunk(byte[] file,int offset,int len){return null;};
        //Return the chunk at offset. Cache the container
        private byte[] readChunk(Offset offset){
            return new byte[1];
        }
        private static ChunkFile fromFile(String filename) throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
            ChunkFile result= (ChunkFile) io.readObject();
            io.close();
            return result;
        };
        public void initRead(){

        }
        public void initWrite(){

        }
        void toFile(String filename) throws IOException {
            ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(filename));
            io.writeObject(this);
            io.close();
        }
    }
    private static class FileRecipe implements Serializable{
        private int totalLength;
        private ArrayList<Offset> chunks;
        private String filename;
        public static  FileRecipe fromFile(String filename) throws IOException, ClassNotFoundException {
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

    private static void download(String filename,String localFileName) throws IOException, ClassNotFoundException {
        FileRecipe fileRecipe=FileRecipe.fromFile(filename);
        ChunkFile chunkFile=ChunkFile.fromFile(ChunkFile.ChunkFileName);
        chunkFile.initRead();
        ByteArrayOutputStream bio=new ByteArrayOutputStream(fileRecipe.totalLength);
        for(Offset offset:fileRecipe.chunks){
            bio.write(chunkFile.readChunk(offset));
        }
        File outFile=new File(localFileName);
        FileOutputStream fout=new FileOutputStream(outFile);
        fout.write(bio.toByteArray());
        bio.close();
        fout.close();
    }
    public  static void main(String[] args) throws IOException, ClassNotFoundException {
        if("upload".equals(args[1])){

        }
        else if("download".equals(args[1])){
            final String fileToDownload=args[2];
            final String localFileName=args[3];
            download(fileToDownload,localFileName);
        }
        else{
            System.out.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> or java MyDedup download <file_to_download> <local_file_name>");
        }


    }
}