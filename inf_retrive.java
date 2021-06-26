import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Pattern;

public class InformationRetrieval {

    public static class Word implements WritableComparable<Word>{

        enum Task{
            TASK1, TASK2, TASK3
        }

        private Text word,docName;
        private IntWritable n,totalN;

        public Word(){
            this.word = new Text();
            this.docName = new Text();
            this.n = new IntWritable();
            this.totalN = new IntWritable();
        }

        public void set(Text word, Text docName){
            this.word.set(word);
            this.docName.set(docName);
        }

        public void set(Text word, Text docName,IntWritable n){
            this.word.set(word);
            this.docName.set(docName);
            this.n = n;
        }
        
        public void set(Text word, Text docName, IntWritable n, IntWritable N){
            this.word.set(word);
            this.docName.set(docName);
            this.n = n;
            this.totalN = N;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

            word.write(dataOutput);
            docName.write(dataOutput);
            n.write(dataOutput);
            totalN.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

            word.readFields(dataInput);
            docName.readFields(dataInput);
            n.readFields(dataInput);
            totalN.readFields(dataInput);
        }

        public Text getWord() {
            return word;
        }

        public void setWord(Text word) {
            this.word = word;
        }

        public Text getDocName() {
            return docName;
        }

        public void setDocName(Text docName) {
            this.docName = docName;
        }

        public IntWritable getN() {
            return n;
        }

        public void setN(IntWritable n) {
            this.n = n;
        }

        public IntWritable getTotalN() {
            return totalN;
        }

        public void setTotalN(IntWritable totalN) {
            this.totalN = totalN;
        }

        // separate toString format for each task
        public String toString(Task task) {
            String retString = "";
                switch (task){
                    case TASK1:
                        retString =  word +"\t"+ docName;
                        break;
                    case TASK2:
                        retString =   word +"\t"+ docName+"\t"+n;
                        break;
                    case TASK3:
                        retString =  word +"\t"+ docName+"\t"+n+"\t"+totalN;
                        break;
                }
            return retString;
        }

        @Override
        public int compareTo(Word o) {
            int i = this.word.compareTo(o.word);
            if(i != 0)
                return i;

            i = this.docName.compareTo(o.docName);
            if(i != 0)
                return i;

            i = this.n.compareTo(o.n);
            if(i != 0)
                return i;

            return this.totalN.compareTo(o.totalN);
        }
    }

    // Mapper for Task 1
    public static class IrMap extends Mapper<Object, Text, Word, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static final List<String> stopWordList = Arrays.asList("a", "acaba", "alt\u0131", "ama", "ancak", "art\u0131k", "asla", "asl\u0131nda", "az", "b", "bana", "bazen", "baz\u0131", "baz\u0131lar\u0131", "baz\u0131s\u0131", "belki", "ben", "beni",
                                                                 "benim", "be\u015F", "bile", "bir", "bir\u00E7o\u011Fu", "bir\u00E7ok", "bir\u00E7okları", "biri", "birisi", "birka\u00E7", "birka\u00E7ı", "bir\u015Fey", "bir\u015Feyi", "biz", "bize", "bizi", "bizim",
                                                                 "b\u00F6yle", "b\u00F6ylece", "bu", "buna", "bunda", "bundan", "bunu", "bunun", "burada", "b\u00FCt\u00FCn", "c", "\u00E7", "\u00E7o\u011Fu", "\u00E7o\u011Funa", "\u00E7o\u011Funu", "\u00E7ok", "\u00E7\u00FCnk\u00FC", "d", "da", "daha", "de", "de\u011Fil",
                                                                 "demek", "di\u011Fer", "di\u011Feri", "di\u011Ferleri", "diye", "dokuz", "dolay\u0131", "d\u00F6rt", "e", "elbette", "en", "f", "fakat", "falan", "felan", "filan", "g", "gene",
                                                                 "gibi", "\u011F", "h", "hâlâ", "hangi", "hangisi", "hani", "hatta", "hem", "hen\u00FCz", "hep", "hepsi", "hepsine", "hepsini", "her", "her biri", "herkes", "herkese", "herkesi",
                                                                 "hi\u00E7", "hi\u00E7 kimse", "hi\u00E7biri", "hi\u00E7birine", "hi\u00E7birini", "ı", "i", "i\u00E7in", "i\u00E7inde", "iki", "ile", "ise", "i\u015Fte", "j", "k", "ka\u00E7", "kadar", "kendi", "kendine",
                                                                 "kendini", "ki", "kim", "kime", "kimi", "kimin", "kimisi", "l", "m", "madem", "m\u0131", "mi", "mu", "m\u00FC", "n", "nas\u0131l", "ne", "ne kadar", "ne zaman", "neden", "nedir", "nerde",
                                                                 "nerede", "nereden", "nereye", "nesi", "neyse", "ni\u00E7in", "niye", "o", "on", "ona", "ondan", "onlar", "onlara", "onlardan", "onlar\u0131n", "onu", "onun",
                                                                 "orada", "oysa", "oysaki", "\u00F6", "\u00F6b\u00FCr\u00FC", "\u00F6n", "\u00F6nce", "\u00F6t\u00FCr\u00FC", "\u00F6yle", "p", "r", "ra\u011Fmen", "s", "sana", "sekiz", "sen", "senden", "seni", "senin", "siz", "sizden",
                                                                 "size", "sizi", "sizin", "son", "sonra", "\u015F", "\u015Fayet", "\u015Fey", "\u015Feyden", "\u015Feye", "\u015Feyi", "\u015Feyler", "\u015Fimdi", "\u015F\u00F6yle", "\u015Fu", "\u015Funa", "\u015Funda", "\u015Fundan", "\u015Funlar",
                                                                 "\u015Funu", "\u015Funun", "t", "tabi", "tamam", "t\u00FCm", "t\u00FCm\u00FC", "u", "\u00FC", "\u00FC\u00E7", "\u00FCzere", "v", "var", "ve", "veya", "veyahut", "y", "ya", "ya da", "yani", "yedi", "yerine",
                                                                 "yine", "yoksa", "z", "zaten", "zira");

        Word wordObject;

        String excludedTweetPattern = "[^a-zA-Z\u00E7\u011F\u0131\u00F6\u015F\u00FC\u00C7\u011E\u0130\u00D6\u015E\u00DC0-9]+";
        Pattern p = Pattern.compile(excludedTweetPattern);

        // Parses the words in each tweet in the text file
        private void getTweetTextWords(StringBuilder stringBuilder, Context context) throws IOException, InterruptedException{
            String[] columns = stringBuilder.toString().split("\t");
            if(columns.length != 0){
                // Replaces unwanted characters specified in the pattern with spaces
                String formattedTweet = p.matcher(columns[columns.length-1]).replaceAll(" ");
                String[] words = formattedTweet.split(" ");
                for( String word: words){
                    word = word.trim();
                    word = word.toLowerCase(new Locale("tr","TR"));
                    // If the length of the word is greater than 2 and it is not in the stopWordList, send that word to the reducer
                    if(word.length() > 2 && !stopWordList.contains(word)){
                        Text preProcessedWord = new Text(word);
                        FileSplit fileSplit = (FileSplit)context.getInputSplit();
                        String filename = fileSplit.getPath().getName(); // Returns the name of the file being read on
                        Text docName = new Text(filename);

                        wordObject = new Word();
                        wordObject.set(preProcessedWord,docName);

                        context.write(wordObject,one);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] lines = value.toString().split("\n");
            StringBuilder lineHolder = new StringBuilder(""); // It keeps all tweet information until it sees "$PYTWEET".
            for (String line : lines){
                if(line.startsWith("$PYTWEET$")){
                    if(lineHolder.length() != 0){
                        // A new tweet has been encountered and there is a tweet previously held in the lineHolder.
                        // So send the tweet held in the lineHolder to the method for parsing.
                        getTweetTextWords(lineHolder,context);
                    }
                    lineHolder = new StringBuilder(line);
                }else{ // if the next line does not start with "$PYTWEET", it is appended to the existing line.
                    lineHolder.append(" ").append(line);
                }
            }
            // when the loop finishes, for the last line of the tweet, the method is called manually to parse the words in lineHolder
            getTweetTextWords(lineHolder,context);
        }
    }

    // Reducer for Task 1
    public static class IrReduce extends Reducer<Word, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Word key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // keeps track of how many times the word appears in the document
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(new Text(key.toString(Word.Task.TASK1)), result);
        }
    }

    // Mapper for Task 2
    // It reads the output file of the first task as input and parses it.
    public static class IrMap2 extends Mapper<Object,Text,Text,Word>{
        Word wordObject;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] lines = value.toString().split("\n");
            for(String line: lines){
                String[] columns = line.split("\t");
                if(columns.length == 3){
                    String word = columns[0];
                    String docName = columns[1];

                    int n = Integer.parseInt(columns[2]);
                    IntWritable iwN = new IntWritable();
                    iwN.set(n);

                    wordObject = new Word();
                    wordObject.set(new Text(word),new Text(docName),iwN);
                    // Since the total number of words in a document will be found in the next reducer stage, the name of the document is sent as the key.
                    context.write(new Text(docName),wordObject);
                }
            }
        }
    }

    // Reducer for Task 2
    public static class IrReduce2 extends Reducer<Text,Word,Text,IntWritable>{
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<Word> values, Context context) throws IOException, InterruptedException {

            // After the total number of words in the document is found,
            // we need to iterate through all the word objects with their extra information to write the result to the output.
            // Cache arraylist is used for this reason.
            ArrayList<Word> cache = new ArrayList<>();
            int sum=0; // keeps the total number of words in the document

            for(Word word: values){
                Word w = new Word();
                w.set(new Text(word.getWord()),new Text(word.getDocName()),new IntWritable(word.getN().get()));
                cache.add(w);
                sum+= word.getN().get();
            }
            result.set(sum);

            for(Word word: cache)
                context.write(new Text(word.toString(Word.Task.TASK2)),result);
        }
    }

    // Mapper for Task 3
    // It reads the output file of the second task as input and parses it.
    public static class IrMap3 extends Mapper<Object,Text,Text,Word>{
        Word wordObject;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] lines = value.toString().split("\n");
            for(String line: lines){
                String[] columns = line.split("\t");
                if(columns.length == 4){
                    String word = columns[0];
                    String docName = columns[1];

                    int n = Integer.parseInt(columns[2]);
                    int totalN = Integer.parseInt(columns[3]);
                    IntWritable iwN = new IntWritable();
                    IntWritable iwTotalN = new IntWritable();
                    iwN.set(n);
                    iwTotalN.set(totalN);

                    wordObject = new Word();
                    wordObject.set(new Text(word),new Text(docName),iwN,iwTotalN);
                    // Since we want to find out how many times a word has used in a document, the words are sent to the reducer as a key.
                    context.write(new Text(word),wordObject);
                }
            }
        }
    }

    // Reducer for Task 3
    public static class IrReduce3 extends Reducer<Text,Word,Text,IntWritable>{
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<Word> values, Context context) throws IOException, InterruptedException {

            //used to iterate through all word objects for a second time.
            ArrayList<Word> cache = new ArrayList<>();
            int sum=0; // keeps how many times the word is used in the corpus

            for(Word word: values){
                Word w = new Word();
                w.set(new Text(word.getWord()),new Text(word.getDocName()),new IntWritable(word.getN().get()),new IntWritable(word.getTotalN().get()));
                cache.add(w);
                sum+= word.getN().get();
            }

            result.set(sum);

            for(Word word: cache){
                context.write(new Text(word.toString(Word.Task.TASK3)),result);
            }
        }
    }

    // Mapper for Task 4
    // It reads the output file of the third task as input and parses it.
    public static class IrMap4 extends Mapper<Object,Text,Word,DoubleWritable>{
        Word wordObject;
        private final static int D = 19; // number of documents
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] lines = value.toString().split("\n");
            for(String line: lines){
                String[] columns = line.split("\t");
                if(columns.length == 5){
                    String word = columns[0];
                    String docName = columns[1];

                    int n = Integer.parseInt(columns[2]);
                    int totalN = Integer.parseInt(columns[3]);
                    int m = Integer.parseInt(columns[4]);

                    DoubleWritable tfIdf = new DoubleWritable();
                    tfIdf.set(( (double) n /(double) totalN ) * (Math.log( (double)D / (double)m )));

                    wordObject = new Word();
                    wordObject.set(new Text(word),new Text(docName));
                    context.write(wordObject,tfIdf);
                }
            }
        }
    }

    // Reducer for Task 4
    public static class IrReduce4 extends Reducer<Word,DoubleWritable,Text,Text>{

        @Override
        protected void reduce(Word key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            for(DoubleWritable dw: values){
                NumberFormat nf = NumberFormat.getInstance();
                nf.setMaximumFractionDigits(8); // we take the decimal part of the TF*IDF as 8 digits
                nf.setGroupingUsed(false);
                // results are written as "(word, docName), tfIdf" in the final output file
                context.write(new Text(key.toString(Word.Task.TASK1)),new Text(nf.format(dw.get())));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJar("InformationRetrieval.jar");
        job1.setJarByClass(InformationRetrieval.class);
        job1.setMapperClass(IrMap.class);
        job1.setReducerClass(IrReduce.class);
        job1.setMapOutputKeyClass(Word.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true); // the second task runs after the first task ends.

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Job2");
        job2.setJarByClass(InformationRetrieval.class);
        job2.setMapperClass(IrMap2.class);
        job2.setReducerClass(IrReduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Word.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true); // the third task runs after the second task ends.

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3,"Job3");
        job3.setJarByClass(InformationRetrieval.class);
        job3.setMapperClass(IrMap3.class);
        job3.setReducerClass(IrReduce3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Word.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.waitForCompletion(true); // the fourth task runs after the third task ends.

        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4,"Job4");
        job4.setJarByClass(InformationRetrieval.class);
        job4.setMapperClass(IrMap4.class);
        job4.setReducerClass(IrReduce4.class);
        job4.setMapOutputKeyClass(Word.class);
        job4.setMapOutputValueClass(DoubleWritable.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
