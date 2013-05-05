package mahout.utils.normalizer.feature;

import org.apache.mahout.math.Vector;

/**
 * 
 * @author Necati Demir <ndemir@demir.web.tr>
 */
abstract public class FeatureNormalizer {

    abstract public Vector normalize(int columnIndex, Vector columnVector);

}
