


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

//"/data/mydedup.index"

/*
 test small:
 upload 4 8 8 10 input/number.txt
 download input/number.txt output/number.txt
 upload 4 8 8 10 input/number2.txt
 download input/number2.txt output/number2.txt
 test large:
 KJV12.TXT: 4.7 mb
 KJV12-2.TXT: 2.4 mb
 upload 32 512 1024 257 input/KJV12.TXT
 download input/KJV12.TXT output/KJV12.txt
 upload 32 1024 2048 13 input/KJV12-2.TXT
 download input/KJV12-2.TXT output/KJV12-2.txt
 upload 32 512 1024 257 input/histories.txt
 upload 32 512 1024 257 input/histories-shift.txt
 download input/KJV12-2.TXT output/KJV12-2.txt
*/
// create data/ manually! create directories
//Test file: D:\Desktop\CSCI4180\asg\asg1
//Test
class MyDedup{

    static int min_chunk = 0;
    static int avg_chunk = 0;
    static int max_chunk = 0;
    static int D = 0;
    static String fileToUpload = null;
    static int[] base_pow = null;
    private static class MetaData implements Serializable{
        private static final long serialVersionUID=3463463463L;
        int numFile;
        int totalBytes;
        int totalChunks;
        int dedupBytes;
        int dedupChunks;
        int numContainers;

        public MetaData(int numFile, int totalBytes, int totalChunks, int dedupBytes, int dedupChunks, int numContainers) {
            this.numFile = numFile;
            this.totalBytes = totalBytes;
            this.totalChunks = totalChunks;
            this.dedupBytes = dedupBytes;
            this.dedupChunks = dedupChunks;
            this.numContainers = numContainers;
        }

        public static MetaData emptyMetaData() {
            return new MetaData(0,0,0,0,0,0);
        }
        /*public static MetaData fromFile() throws IOException, ClassNotFoundException {
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
         */
    }
    //SHA1 fingerprint class: can generate fingerprint with static method
    private static class FingerPrint implements Serializable {
        byte[] sha1;
        static final private MessageDigest md;
        private static final long serialVersionUID=36829907L;

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
    private static class Offset implements Serializable {
        private static final long serialVersionUID=1125884L;
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
        MetaData metaData ;
        HashMap<FingerPrint,Offset> storage;

        private static final long serialVersionUID=1241241;
        private final static String fingerPrintIndexPath="./data/mydedup.index";
        //public final static String fingerPrintIndexPath="./data/fingerprint.index";
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
             metaData = MetaData.emptyMetaData();
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
        private static final long serialVersionUID=4335352;
        private static final String ChunkFileName="data/storage.bin";
        private static final String ContainerName="data/container.index";
        private static final int containerSize =  1048576; // 1 MiB = 2^20 bytes
        //buffer
        transient private ByteBuffer bf_in;
        transient private ByteBuffer bf_out;
        //the final bit of each container
        private ArrayList<Integer> containerEndLoc; // real container_num = containerEndLoc.size() - 1 (save an extra container 0)
        transient private BufferedInputStream in;
        transient private BufferedOutputStream out;
        transient private RandomAccessFile fis;
        transient private FileOutputStream fos;
        transient private byte[] byte_in;
        transient private int current_container;
        transient private FileChannel rChannel;
        transient private FileChannel wChannel;
        public ChunkFile(){
            containerEndLoc = new ArrayList<Integer>();
            containerEndLoc.add(0);
        }
        // flush buffer, update containerEndLoc
        private void writeContainer() throws IOException{
            bf_out.flip();
            wChannel.write(bf_out);

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
        private byte[] readChunk(Offset offset) throws IOException{
            byte[] chunk_byte = new byte[offset.len];
            // check if chunk is in the current container
            if(current_container != offset.container) { // load the container of the chunk to buffer
                int container_start = containerEndLoc.get(offset.container - 1);
                int container_end = containerEndLoc.get(offset.container) - 1;
                int container_size = container_end - container_start + 1;
                bf_in.clear();
                bf_in.limit(container_size);
                rChannel.position(container_start);
                rChannel.read(bf_in);

                current_container = offset.container;
            }
            //read from current container
            bf_in.limit(offset.offset + offset.len);
            bf_in.position(offset.offset);

            bf_in.get(chunk_byte);

            return chunk_byte;
        }
        private static ChunkFile fromFile(String filename) throws IOException, ClassNotFoundException {
            ObjectInputStream io=new ObjectInputStream(new FileInputStream(filename));
            ChunkFile result= (ChunkFile) io.readObject();
            io.close();
            return result;
        };
        public void initRead(String filename, int file_len) throws IOException{
            fis = new RandomAccessFile(ChunkFileName, "r");
            bf_in = ByteBuffer.allocate(containerSize); // 1 mb, load container
            current_container = 0;
            rChannel = fis.getChannel();

        }
        public void initWrite() throws FileNotFoundException{
            //fos = new RandomAccessFile(ChunkFileName, "r");

            fos = new FileOutputStream(ChunkFileName, true);
            //out = new BufferedOutputStream(fos);
            bf_out = ByteBuffer.allocate(containerSize);
            wChannel = fos.getChannel();
            //end write close?
        }
        public void endRead() throws IOException{
            //in.close();
            fis.close();
        }
        public void endWrite() throws IOException{
            //out.close();
            fos.close();
        }
        void toFile(String filename) throws IOException {
            ObjectOutputStream io=new ObjectOutputStream(new FileOutputStream(filename));
            io.writeObject(this);
            io.close();
        }
    }
    private static class FileRecipe implements Serializable{
        private static final long serialVersionUID=38192063;
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

    public static String toDataStoragePath(String input){
        if(input.substring(0,1).equals("./")){
            return input.substring(2).replace('/','-');
        }
        return input.replace('/','-');
    }
    private static void upload() throws IOException, ClassNotFoundException{
        // load fingerprint index and metadata
        // create file recipe
        // load container index
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
            File data_dir = new File("data/");
            data_dir.mkdir();
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

        file_recipe.filename = fileToUpload;
        file_recipe.totalLength = file_bytes.length;

        //record metadata
        int newBytes = 0;
        int newChunks = 0;
        int newDedupBytes = 0;
        int newDedupChunks = 0;

        // start RFP: window_size = min_chunk; modulus q = avg_chunk; base = D
        int start = 0;
        int end;
        int rf;
        int rf_prev = avg_chunk; // set rf_prev to modulus if not to use rf_prev

        for ( int i = 0; i <= file_bytes.length-min_chunk ; i++){

            rf = rabinFingerprint(file_bytes, i, rf_prev);
            rf_prev = rf;
            if((rf & (avg_chunk - 1)) == 0 || i - start + min_chunk == max_chunk) {

                // add anchor after the slide window, create chunk from bytes[start] to bytes[end] inclusive

                end = i + min_chunk-1;
                FingerPrint chunk_fingerprint = FingerPrint.SHA1(file_bytes, start, end - start + 1);
                newDedupChunks++;
                newDedupBytes += end - start + 1;

                Offset chunk_offset;
                if(f_index.containsKey(chunk_fingerprint) ){
                    chunk_offset = f_index.get(chunk_fingerprint);
                }else{
                    newChunks++;
                    newBytes+= end - start + 1;
                    chunk_offset = container_index.appendChunk(file_bytes, start, end - start + 1);
                    f_index.put(chunk_fingerprint,chunk_offset);
                }
                file_recipe.chunks.add(chunk_offset);

                start = end + 1;
                i = end;
                rf_prev = avg_chunk;
            }
        }
        if(start <= file_bytes.length - 1){ // create a chunk for the tail of the file
            FingerPrint chunk_fingerprint = FingerPrint.SHA1(file_bytes, start, file_bytes.length - start);

            newDedupChunks++;
            newDedupBytes += file_bytes.length - start;
            Offset chunk_offset;
            if(f_index.containsKey(chunk_fingerprint) ){
                chunk_offset = f_index.get(chunk_fingerprint);
            }else{
                newChunks++;
                newBytes += file_bytes.length - start;
                chunk_offset = container_index.appendChunk(file_bytes, start, file_bytes.length - start);
                f_index.put(chunk_fingerprint, chunk_offset);
            }
            file_recipe.chunks.add(chunk_offset);
        }
        // end of file, flush the container
        if(container_index.bf_out.position() > 0){
            container_index.writeContainer();
        }
        f_index.metaData.numFile += 1;
        f_index.metaData.dedupChunks += newDedupChunks;
        f_index.metaData.totalChunks += newChunks;
        f_index.metaData.dedupBytes += newDedupBytes;
        f_index.metaData.totalBytes += newBytes;
        f_index.metaData.numContainers = container_index.containerEndLoc.size()-1;
        double ratio = (double)f_index.metaData.dedupBytes / (double)f_index.metaData.totalBytes;

        container_index.endWrite();
        f_index.toFile();
        file_recipe.toFile(file_recipe_path);
        container_index.toFile(ChunkFile.ContainerName);


        System.out.format(
                "Report Output:"+
                "\nTotal number of files that have been stored: " +f_index.metaData.numFile+
                        "\nTotal number of pre-deduplicated chunks in storage: " +f_index.metaData.dedupChunks+
                        "\nTotal number of unique chunks in storage: " +f_index.metaData.totalChunks+
                        "\nTotal number of bytes of pre-deduplicated chunks in storage: " +f_index.metaData.dedupBytes+
                        "\nTotal number of bytes of unique chunks in storage: " +f_index.metaData.totalBytes+
                        "\nTotal number of containers in storage: " +f_index.metaData.numContainers+
                        "\nDeduplication ratio: %.2f\n", ratio
        );
    }

    private static void download(String filename,String localFileName) throws IOException, ClassNotFoundException {
        String file_recipe_path = "data/"+filename.replace('/','-');// create directory?

        FileRecipe fileRecipe=FileRecipe.fromFile(file_recipe_path);
        ChunkFile chunkFile=ChunkFile.fromFile(ChunkFile.ContainerName);
        chunkFile.initRead(file_recipe_path, fileRecipe.totalLength);
        ByteArrayOutputStream bio=new ByteArrayOutputStream(fileRecipe.totalLength);
        for(Offset offset:fileRecipe.chunks){
            bio.write(chunkFile.readChunk(offset));
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