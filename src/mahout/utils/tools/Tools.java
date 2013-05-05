package mahout.utils.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;

/**
 *
 * @author Necati Demir <ndemir@demir.web.tr>
 */
public class Tools{

    public static List<NamedVector> getNamedVectorList(FileSystem fs, Path csvFilePath,
            int columnNameIndex) throws Exception{
        String[] lines = Tools.getLines(fs, csvFilePath);
        Text idText = new Text();
        int columnSize = lines[0].split(",").length-1;
        ArrayList namedVectorList = new ArrayList();

        for (int i=0; i<lines.length; i++){
            idText.clear();
            String[] columns = lines[i].split(",");

            idText.set( (columnNameIndex > -1) ? columns[columnNameIndex] :
                String.valueOf(i) );


            RandomAccessSparseVector v = new RandomAccessSparseVector(
                    columnSize);
            int columnIndex = 0;
            double d[] = new double[columnSize];
            for (int j=0; j<columns.length; j++){
                if (j == columnNameIndex) continue;
                d[columnIndex] = Double.parseDouble(columns[j]);
                columnIndex++;
            }
            v.assign(d);
            namedVectorList.add( new NamedVector(v, idText.toString()) );

        }

        return namedVectorList;

    }

    public static String[] getLines(FileSystem fs, Path p) throws Exception{
        InputStream in = null;
        OutputStream oStream = new OutputStream() {

            StringBuilder sb = new StringBuilder();

            @Override
            public void write(int b) throws IOException {
                sb.append((char) b);
            }

            @Override
            public String toString(){
                return sb.toString();
            }

        };

        try {
          in = fs.open(p);
          IOUtils.copyBytes(in, oStream, 4096, false);
        } finally {
          IOUtils.closeStream(in);
        }


        return oStream.toString().split("\n");

    }

    public static Path[] getPathArr(FileSystem fs, Path p) throws Exception{

        Path[] pathArr;

        FileStatus fileStatus = fs.getFileStatus(p);
        if (fileStatus.isDir()) {
          pathArr = FileUtil.stat2Paths(fs.listStatus(p, new OutputFilesFilter()));
        } else {
          FileStatus[] inputPaths = fs.globStatus(p);
          pathArr = new Path[inputPaths.length];
          int i = 0;
          for (FileStatus fstatus : inputPaths) {
            pathArr[i++] = fstatus.getPath();
          }
        }

        return pathArr;

    }


}
