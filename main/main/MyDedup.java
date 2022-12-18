package main;


import java.io.*;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

//"/data/mydedup.index"

/*
 test:
 upload 4 8 8 10 input/number.txt
 download input/number.txt output/number.txt
 upload 4 8 8 10 input/number2.txt
 download input/number2.txt output/number2.txt
*/



//Test file: D:\Desktop\CSCI4180\asg\asg1
class MyDedup{

    static int min_chunk = 0;
    static int avg_chunk = 0;
    static int max_chunk = 0;
    static int D = 0;
    static String fileToUpload = null;
    static int[] base_pow = null;
    private static class MetaData implements Serializable{
        int numFile;
        int totalBytes;
        int totalChunks;
        int dedupBytes;
        int dedupChunks;
        // num of container? dedup ratio?

        private final static String fingerPrintIndexPath="./data/mydedup.index";

        public MetaData(int numFile, int totalBytes, int totalChunks, int dedupBytes, int dedupChunks) {
            this.numFile = numFile;
            this.totalBytes = totalBytes;
            this.totalChunks = totalChunks;
            this.dedupBytes = dedupBytes;
            this.dedupChunks = dedupChunks;
        }

        public static MetaData emptyMetaData() {
            return new MetaData(0,0,0,0,0);
        }
        public static MetaData fromFile() throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(fingerPrintIndexPath));
            MetaData result= (MetaData) io.readObject();
            io.close();
            return result;
        };
        void toFile() throws IOException {
            ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(fingerPrintIndexPath));
            io.writeObject(this);
            io.close();
        }
    }
    //SHA1 fingerprint class: can generate fingerprint with static method
    private static class FingerPrint implements Serializable{
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
        static FingerPrint SHA1(byte[] file,int offset,int len)  {
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
    private static class Offset implements Serializable{
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

    }
    private  static class FingerIndex implements Serializable{
        HashMap<FingerPrint,Offset> storage;
        transient public final static String fingerPrintIndexPath="./data/fingerprint.index";
         private static FingerIndex fromFile() throws IOException, ClassNotFoundException {
             ObjectInputStream io=new ObjectInputStream(new FileInputStream(fingerPrintIndexPath));
             FingerIndex result= (FingerIndex) io.readObject();
             io.close();
             return result;
         }
         void toFile() throws IOException {
             ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(fingerPrintIndexPath));
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
        Boolean containsKey(FingerPrint sha1){
            return storage.containsKey(sha1);
        };
        Offset put(FingerPrint pf,Offset offset){
             return storage.put(pf,offset);
         };
    }
    private static class ChunkFile implements Serializable{
        private static final String ChunkFileName="data/storage.bin";
        private static final String ContainerName="data/container.index";
        private static final int containerSize = 1048576; // 1 MiB = 2^20 bytes
        //buffer
        transient private ByteBuffer bf_in;
        transient private ByteBuffer bf_out;
        //the final bit of each container
        private ArrayList<Integer> containerEndLoc; // real container_num = containerEndLoc.size() - 1 (save an extra container 0)
        transient private BufferedInputStream in;
        transient private BufferedOutputStream out;
        transient private FileInputStream fis;
        transient private FileOutputStream fos;
        transient private byte[] byte_in;
        public ChunkFile(){
            containerEndLoc = new ArrayList<Integer>();
            containerEndLoc.add(0);
        }
        // flush buffer, update containerEndLoc
        private void writeContainer() throws IOException{
            bf_out.flip();
            byte[] byte_out = new byte[bf_out.limit()];
            bf_out.get(byte_out);

            out.write(byte_out);
            out.flush();

            int prev_loc = containerEndLoc.get(containerEndLoc.size() - 1);
            containerEndLoc.add(bf_out.limit() + prev_loc);

            bf_out.clear();
        }
        //Put the chunk into the buffer. If buffer is full, flush it. return the container number
        //and the offset from the start of the file
        private Offset appendChunk(byte[] file,int offset,int len) throws  IOException{

            ByteBuffer temp = ByteBuffer.wrap(file, offset, len);
            if(bf_out.remaining() < len){ // buffer is full, flush the current container and clear bf_out, start a new container
                writeContainer();
            }
            Offset chunk_offset = new Offset(containerEndLoc.size() ,bf_out.position(),len);
            bf_out.put(temp);

            if(file.length == offset + len){ // tail of the file, flush the current container and clear bf_out
                writeContainer();
            }

            return chunk_offset;
        }
        //Return the chunk at offset. Cache the container
        private byte[] readChunk(Offset offset, int file_len) throws IOException{
            bf_in = ByteBuffer.allocate(4000);
            int storage_offset = containerEndLoc.get(offset.container - 1) + offset.offset;
            byte[] chunk = new byte[40];
            //in.skip(storage_offset);
            in.read(chunk,storage_offset, offset.len);
            ByteBuffer temp = ByteBuffer.wrap(chunk, storage_offset, offset.len);
            bf_in.put(temp);
            bf_in.flip();
            byte[] byte_in = new byte[bf_in.limit()];
            bf_in.get(byte_in);
            return byte_in;
        }
        private static ChunkFile fromFile(String filename) throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
            ChunkFile result= (ChunkFile) io.readObject();
            io.close();
            return result;
        };
        public void initRead(String filename, int file_len) throws IOException{
            fis = new FileInputStream(filename);
            in = new BufferedInputStream(fis);

        }
        public void initWrite() throws FileNotFoundException{
            fos = new FileOutputStream(ChunkFileName, true);
            out = new BufferedOutputStream(fos);
            bf_out = ByteBuffer.allocate(containerSize);
            //end write close?
        }
        public void endRead() throws IOException{
            in.close();
            fis.close();
        }
        public void endWrite() throws IOException{
            out.close();
            fos.close();
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
        public FileRecipe(){
            chunks = new ArrayList<Offset>();
        }
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

        public void setTotalLength(int totalLength) {
            this.totalLength = totalLength;
        }
        public ArrayList<Offset> getChunks() {
            return this.chunks;
        }
        public void setFilename(String filename) {
            this.filename = filename;
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

    private static void initBasePower(){
        //base_pow[i] = (D ^ i) mod avg_chunk
        base_pow = new int[min_chunk];
        base_pow[0] = 1;
        for (int i = 1; i < min_chunk; i++){
            base_pow[i] = (base_pow[i-1] * D) % avg_chunk;
        }
    }
    private static int rabinFingerprint(byte[] file_bytes, int s, int prev_rf){
        int rf = 0;
        if (prev_rf == avg_chunk ){ // calculate rf[i] base on file_bytes[i] ~ file_bytes[i+min_chunk-1]
            int pow = min_chunk-1;
            for (int i = s; i < s + min_chunk; i++){
                rf = (rf + (file_bytes[i] * base_pow[pow]) % avg_chunk) % avg_chunk;
                pow--;
            }
        }else{ // prev_rf exist, calculate rf[i] base on rf[i-1],
            rf = Math.floorMod(prev_rf - Math.floorMod(base_pow[min_chunk - 1] * file_bytes[s-1], avg_chunk), avg_chunk);
            rf = Math.floorMod(D * rf + file_bytes[s+min_chunk-1], avg_chunk);
        }
        return rf;
    }

    private static void upload() throws IOException, ClassNotFoundException{
        // load fingerprint index
        File finger_index_file = new File(FingerIndex.fingerPrintIndexPath);
        File container_index_file = new File(ChunkFile.ContainerName);
        String file_recipe_path = new String("data/"+fileToUpload.replace('/','-'));// create directory?
        File file_recipe_file = new File(file_recipe_path);
        FingerIndex f_index = new FingerIndex();
        FileRecipe file_recipe = new FileRecipe();
        ChunkFile container_index = new ChunkFile();
        if (finger_index_file.exists()){
            f_index = FingerIndex.fromFile();
        }else{
            finger_index_file.createNewFile();
        }
        file_recipe_file.createNewFile();

        if (container_index_file.exists()){
            container_index = ChunkFile.fromFile(ChunkFile.ContainerName);
        }else{
            container_index_file.createNewFile();
        }

        container_index.initWrite();
        // read upload file
        File inFile = new File(fileToUpload);
        byte[] file_bytes = readFileBytes(inFile);

        file_recipe.setFilename(fileToUpload);
        file_recipe.setTotalLength(file_bytes.length);

        // start RFP: window_size = min_chunk; modulus q = avg_chunk; base = D
        int start = 0;
        int end;
        int rf;
        int rf_prev = avg_chunk; // set rf_prev to modulus if not to use rf_prev

        for ( int i = 0; i <= file_bytes.length-min_chunk ; i++){

            rf = rabinFingerprint(file_bytes, i, rf_prev);
            rf_prev = rf;
            System.out.println("analyzing i= "+i);
            if((rf & (avg_chunk - 1)) == 0 || i - start + min_chunk == max_chunk) {

                System.out.println("\ni: " + i);
                System.out.println("byte[i]: " + file_bytes[i]);
                System.out.println("byte[i] in binary: " + Integer.toBinaryString(file_bytes[i]));
                System.out.println("rf: " + rf);
                System.out.println("rf in binary: " + Integer.toBinaryString(rf));
                System.out.println("avg_chunk in binary: " + Integer.toBinaryString(avg_chunk - 1));
                System.out.println("binary: " + Integer.toBinaryString(file_bytes[i] & (avg_chunk - 1)));


                // add anchor after the slide window, create chunk from bytes[start] to bytes[end] inclusive
                end = i + min_chunk-1;
                String output = new String(file_bytes, start, end - start + 1);
                FingerPrint chunk_fingerprint = FingerPrint.SHA1(file_bytes, start, end - start + 1);
                System.out.println("output chunk: " + output);
                System.out.println("SHA1: " + chunk_fingerprint.sha1);

                Offset chunk_offset;
                if(f_index.containsKey(chunk_fingerprint) ){
                    chunk_offset = f_index.get(chunk_fingerprint);
                    System.out.println("found! offset is: "+chunk_offset.offset+"  length is: "+chunk_offset.len);
                }else{
                    chunk_offset = container_index.appendChunk(file_bytes, start, end - start + 1);
                    f_index.put(chunk_fingerprint,chunk_offset);
                }
                file_recipe.getChunks().add(chunk_offset);

                start = end + 1;
                i = end;
                rf_prev = avg_chunk;
            }
        }
        if(start <= file_bytes.length - 1){ // create a chunk for the tail of the file
            String output = new String(file_bytes, start, file_bytes.length - start);
            System.out.println("output chunk: " + output);
            FingerPrint chunk_fingerprint = FingerPrint.SHA1(file_bytes, start, file_bytes.length - start);
            System.out.println("SHA1: " + chunk_fingerprint.sha1);

            Offset chunk_offset;
            if(f_index.containsKey(chunk_fingerprint) ){
                chunk_offset = f_index.get(chunk_fingerprint);
                System.out.println("found! offset is: "+chunk_offset.offset+"  length is: "+chunk_offset.len);
            }else{
                chunk_offset = container_index.appendChunk(file_bytes, start, file_bytes.length - start);
                f_index.put(chunk_fingerprint, chunk_offset);
            }
            file_recipe.getChunks().add(chunk_offset);
        }
        container_index.endWrite();
        f_index.toFile();
        file_recipe.toFile(file_recipe_path);
        container_index.toFile(ChunkFile.ContainerName);
    }

    private static void download(String filename,String localFileName) throws IOException, ClassNotFoundException {
        String file_recipe_path = new String("data/"+filename.replace('/','-'));// create directory?

        FileRecipe fileRecipe=FileRecipe.fromFile(file_recipe_path);
        ChunkFile chunkFile=ChunkFile.fromFile(ChunkFile.ContainerName);
        chunkFile.initRead(filename, fileRecipe.totalLength);
        ByteArrayOutputStream bio=new ByteArrayOutputStream(fileRecipe.totalLength);
        for(Offset offset:fileRecipe.chunks){
            bio.write(chunkFile.readChunk(offset, fileRecipe.totalLength));
        }
        chunkFile.endRead();
        File outFile=new File(localFileName);
        FileOutputStream fout=new FileOutputStream(outFile);
        fout.write(bio.toByteArray());
        bio.close();
        fout.close();
    }
    public  static void main(String[] args) throws IOException, ClassNotFoundException {

        if("upload".equals(args[0])){ // args[1]?
            min_chunk = Integer.parseInt(args[1]);
            avg_chunk = Integer.parseInt(args[2]);
            max_chunk = Integer.parseInt(args[3]);
            D = Integer.parseInt(args[4]);
            fileToUpload=args[5];
            if ((min_chunk & min_chunk-1) != 0 || (avg_chunk & avg_chunk-1) != 0 || (max_chunk & max_chunk-1) != 0){ // chunk size is not a power of 2
                System.out.println("The chunk size parameter is required to ba a power of 2");
                System.exit(1);
            }
            initBasePower();
            upload();
        }
        else if("download".equals(args[0])){
            final String fileToDownload=args[1];
            final String localFileName=args[2];
            download(fileToDownload,localFileName);
        }
        else{
            System.out.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> or java MyDedup download <file_to_download> <local_file_name>");
        }



    }
}