package es.unican.electricity.curves.utils;

/**
 * Created by algorrim on 8/03/18.
 */
public class Converter {

    public static Double[] stringArrayToDoubleArray(String[] lines) {
        Double[] result = new Double[lines.length];
        for (int i = 0; i<lines.length; i++){
            result[i] = Double.valueOf(lines[i]);
        }
        return result;
    }

    public static Integer[] stringArrayToIntArray(String[] lines) {
        Integer[] result = new Integer[lines.length];
        for (int i = 0; i<lines.length; i++){
            result[i] = Integer.valueOf(lines[i]);
        }
        return result;
    }
}
