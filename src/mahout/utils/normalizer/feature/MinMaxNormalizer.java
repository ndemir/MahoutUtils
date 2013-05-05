package mahout.utils.normalizer.feature;

import java.util.HashMap;
import org.apache.mahout.math.Vector;

/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
public class MinMaxNormalizer extends FeatureNormalizer {

    private HashMap newMinMaxValues = new HashMap();

    public MinMaxNormalizer(){
    }
    
    public void setMinMax(int columnIndex, double newMin, double newMax){
        double[] d = {newMin, newMax};
        newMinMaxValues.put(columnIndex, d);
    }


    @Override
    public Vector normalize(int columnIndexOfMatrix, Vector columnVector) {

        if (! newMinMaxValues.containsKey(columnIndexOfMatrix)){
            return columnVector;
        }

        double originalMin = columnVector.minValue();
        double originalMax = columnVector.maxValue();

        double[] newminmax = (double[]) newMinMaxValues.get(columnIndexOfMatrix);

        MinMaxDoubleFunction func = new MinMaxDoubleFunction(originalMin,
                originalMax, newminmax[0], newminmax[1]);

        return columnVector.assign(func);
        
    }

}
