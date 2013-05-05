package mahout.utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.util.ArrayList;
import mahout.utils.tools.Tools;
import mahout.utils.normalizer.feature.FeatureNormalizer;
import mahout.utils.normalizer.feature.MinMaxNormalizer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
public class CSV2Sparse extends AbstractJob{

    private Configuration conf;
    private FileSystem fs;

    public static void main(String args[]) throws Exception{
        ToolRunner.run(new Configuration(), new CSV2Sparse(), args);
    }

    public CSV2Sparse() throws IOException{
        conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            Logger.getLogger(CSV2Sparse.class.getName()).log(Level.SEVERE, null,
                    ex);
        }
    }



    public int run(String[] args) throws Exception {
        String Option_columnNameIndex = "columnNameIndex";
        String Option_columnNameIndex_short = "cni";

        String Option_minmaxFeautureNormalization = "fn_minmax";
        String Option_minmaxFeautureNormalization_short = "fn_minmax";

        String Option_zscoreFeatureNormalization = "fn_zscore";
        String Option_zscoreFeatureNormalization_short = "fn_zscore";

        
        
        addInputOption();
        addOutputOption();
        addOption(Option_columnNameIndex, Option_columnNameIndex_short, "What "
                + "colum index shows the name?", false);
        addOption(Option_minmaxFeautureNormalization, 
                Option_minmaxFeautureNormalization_short, "MinMax Feature"
                + " Normalization", false);

        if (parseArguments(args) == null){
            return -1;
        }

        int columnNameIndex = 0;
        if (hasOption(Option_columnNameIndex)){
            columnNameIndex = Integer.parseInt(
                    getOption(Option_columnNameIndex) );

        }

        FeatureNormalizer featureNormalizer = new FeatureNormalizer() {

            @Override
            public Vector normalize(int columnIndex, Vector columnVector) {
                return columnVector;
            }
            
        };
        if (hasOption(Option_minmaxFeautureNormalization)){ //example: -fn_minmax 0:0-1,1:0-1
            MinMaxNormalizer minmaxNormalizer = new MinMaxNormalizer();
            
            String[] minmaxValuesPerColumn = getOption(Option_minmaxFeautureNormalization)
                    .split(",");

            for (String s: minmaxValuesPerColumn){
                int columnIndex = Integer.parseInt( s.split(":")[0] );
                double[] d = new double[2];
                d[0] = Double.parseDouble( s.split(":")[1].split("-")[0] );
                d[1] = Double.parseDouble( s.split(":")[1].split("-")[1] );

                minmaxNormalizer.setMinMax(columnIndex, d[0], d[1]);
            }

            featureNormalizer = minmaxNormalizer;

            
        }


        Path[] pathArr = Tools.getPathArr(fs, getInputPath());

        SequenceFile.Writer seqWriter = SequenceFile.createWriter(fs, conf,
                getOutputPath(), Text.class, VectorWritable.class);

        ArrayList<NamedVector> namedVectorList = new ArrayList<NamedVector>();
        for (Path p: pathArr){
            namedVectorList.addAll( Tools.getNamedVectorList(fs, p, columnNameIndex) );
        }

        int rowSize = namedVectorList.size();
        int columnSize = namedVectorList.get(0).size();
        SparseMatrix matrix = new SparseMatrix(rowSize, columnSize);
        for (int i=0; i<namedVectorList.size(); i++){
            matrix.assignRow(i, namedVectorList.get(i));
        }

        for (int i=0; i<columnSize; i++){
            Vector normalizedColumn = featureNormalizer.normalize(i,
                    matrix.viewColumn(i));
            matrix.assignColumn(i, normalizedColumn);
        }

        for (int i=0; i<rowSize; i++){
            NamedVector v = (NamedVector) matrix.viewRow(i);
            seqWriter.append(new Text( v.getName() ), new VectorWritable(v));
        }

        seqWriter.close();

        return 0;
    }


}
