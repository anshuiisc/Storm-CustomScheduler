package main.java;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by anshushukla on 09/03/16.
 */
public class test {

    public static void main(String[] args) {

        List<String> p=new ArrayList<>();
        p.add("uh#slot2,1");
        p.add("rh#slot3,1");

//        ["uh#slot2,1,1", "rh#slot3,1,1"];

        int totalExecconf=0;
        for(String s1:p){
            totalExecconf+=Integer.parseInt(s1.split(",")[1]);
        }


        System.out.println(totalExecconf);




        Integer[] sortedIDXs  = new Integer[]{10,1,2,3,4};
//        Arrays.sort(sortedIDXs, new Comparator<Integer>() {
//            public int compare(Integer idx1, Integer idx2) {
//                return Double.compare(distances[idx1], distances[idx2]);
//            }
//        });
    }
}
