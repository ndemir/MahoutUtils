package mahout.utils;

import org.apache.hadoop.util.ProgramDriver;

/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
public class Main {

    public static void main(String[] args) throws Throwable {
        ProgramDriver programDriver = new ProgramDriver();
        programDriver.addClass("csv2sparse", mahout.utils.CSV2Sparse.class, "Generate sparse vectors from CSV file(s)");
        programDriver.driver(args);
    }

}
