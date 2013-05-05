package mahout.utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import mahout.utils.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.NamedVector;


/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
public class ClusterDumper extends AbstractJob{

    private Configuration conf;
    private FileSystem fs;

    private static final String Option_writeClusterVectors = "write_cluster_vectors";
    private static final String Option_writeClusterVectors_short = "write_cluster_vectors";

    public static void main(String args[]) throws Exception{
        ToolRunner.run(new Configuration(), new ClusterDumper(), args);
    }

    public ClusterDumper() throws IOException{
        conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            Logger.getLogger(CSV2Sparse.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void writeClusterVectors() throws Exception{
        
        Path paths[] = Tools.getPathArr(fs, getInputPath());

        FSDataOutputStream outputStream = fs.create(new Path(getOption(Option_writeClusterVectors)), true);
        outputStream.writeBytes("vector_name,cluster_name\n");

        IntWritable key = new IntWritable();
        WeightedVectorWritable value = new WeightedVectorWritable();
        for (Path path: paths){
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
            while (reader.next(key, value)){
                outputStream.writeBytes(((NamedVector)value.getVector()).getName()+","+key.toString()+"\n");
            }
            reader.close();
        }

        outputStream.close();

    }


    public int run(String[] args) throws Exception {
        
        addInputOption();
        addOption(Option_writeClusterVectors, Option_writeClusterVectors_short, "Write to a file which vector belongs to which cluster in CSV format");

        if (parseArguments(args) == null) {
          return -1;
        }

        if (hasOption(Option_writeClusterVectors)){
            writeClusterVectors();
        }

        return 0;

    }
    
}
