package mahout.utils.normalizer.feature;

import org.apache.mahout.math.function.DoubleFunction;

/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
class MinMaxDoubleFunction implements DoubleFunction{

    private double originalMin, originalMax, newMin, newMax;

    public MinMaxDoubleFunction(){

    }

    public MinMaxDoubleFunction(double originalMin, double originalMax,
            double newMin, double newMax){
        this.originalMin = originalMin;
        this.originalMax = originalMax;
        this.newMin = newMin;
        this.newMax = newMax;
    }

    public double apply(double d) {
        return ( ( (d-originalMin)/(originalMax-originalMin) )*(newMax-newMin) )+newMin;
    }

}
