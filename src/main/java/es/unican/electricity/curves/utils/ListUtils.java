package es.unican.electricity.curves.utils;

import java.util.List;
import java.util.function.Function;

/**
 * Created by algorrim on 14/03/18.
 */
public class ListUtils {
    static public <E> String mkString(List<E> list, Function<E,String> stringify, String delimiter) {
        int i = 0;
        StringBuilder s = new StringBuilder();
        for (E e : list) {
            if (i != 0) { s.append(delimiter); }
            s.append(stringify.apply(e));
            i++;
        }
        return s.toString();
    }
}