package main.java;

/**
 * Created by anshushukla on 10/03/16.
 */


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;

/**
 * @author Crunchify.com
 */

public class JsonFIleReader {

    @SuppressWarnings("unchecked")
    static String getJsonConfig(String jsonfilepath,String toponame,String boltname) {
        JSONParser parser = new JSONParser();
        JSONObject threadWithConf=null;
        String ExecMappingconfig=null;
        try {

            File f=new File(jsonfilepath);



//            if(f.exists())
//                System.out.println("\n\n\t\tJSON File does not exist in conf *********");
            if(f.exists() && !f.isDirectory())
                {
                    FileReader fr=new FileReader(f);

                Object obj = parser.parse(fr);
                JSONObject jsonObject = (JSONObject) obj;

                JSONObject fullTopoMapping = (JSONObject) jsonObject.get(toponame);
                    if (fullTopoMapping == null) {
                        System.out.println("\t\t\t\t******Check json inside conf folder Topology name Mismatch ******\n\n\n");
                    }
                    else {
                        threadWithConf = (JSONObject) fullTopoMapping.get(boltname);
//                    System.out.println(threadWithConf);
                        if (threadWithConf == null) {
                            System.out.println("\t\t\t\t******Check json inside conf folder Bolt name Mismatch ******\n\n\n");
                        } else {
                            ExecMappingconfig = (String) threadWithConf.get("conf");
                        }
                    }

//                boltList = (JSONObject) jsonObject.get(toponame);
//                if (boltList == null) {
//                    System.out.println("\t\t\t\t******Check json inside conf folder Topology name Mismatch ******\n\n\n");
//                } else {
//                    ExecMappingconfig = (String) boltList.get(boltname);
//                }
//                if (ExecMappingconfig == null) {
//                    System.out.println("\t\t\t\t******Check json inside conf folder Bolt name Mismatch ******");
//                }
//
//                System.out.println("\n\n\t\t\t\tToponame----" + toponame + "-----Boltname-----" + boltname);


            }
            else System.out.println("\n\n\t\t********* JSON File does not exist in conf *********");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ExecMappingconfig;
    }

    @SuppressWarnings("unchecked")
    static String getJsonThreadCount(String jsonfilepath,String toponame,String boltname) {
        JSONParser parser = new JSONParser();
        JSONObject threadWithConf=null;
        String ExecMappingconfig=null;
        try {

            File f=new File(jsonfilepath);



//            if(f.exists())
//                System.out.println("\n\n\t\tJSON File does not exist in conf *********");
            if(f.exists() && !f.isDirectory())
            {
                FileReader fr=new FileReader(f);

                Object obj = parser.parse(fr);
                JSONObject jsonObject = (JSONObject) obj;

                JSONObject fullTopoMapping = (JSONObject) jsonObject.get(toponame);
                if (fullTopoMapping == null) {
                    System.out.println("\t\t\t\t******Check json inside conf folder Topology name Mismatch ******\n\n\n");
                }
                else {
                    threadWithConf = (JSONObject) fullTopoMapping.get(boltname);
//                    System.out.println(threadWithConf);
                    if (threadWithConf == null) {
                        System.out.println("\t\t\t\t******Check json inside conf folder Bolt name Mismatch ******\n\n\n");
                    } else {
                        ExecMappingconfig = (String) threadWithConf.get("threads");
                    }
                }

//                boltList = (JSONObject) jsonObject.get(toponame);
//                if (boltList == null) {
//                    System.out.println("\t\t\t\t******Check json inside conf folder Topology name Mismatch ******\n\n\n");
//                } else {
//                    ExecMappingconfig = (String) boltList.get(boltname);
//                }
//                if (ExecMappingconfig == null) {
//                    System.out.println("\t\t\t\t******Check json inside conf folder Bolt name Mismatch ******");
//                }
//
//                System.out.println("\n\n\t\t\t\tToponame----" + toponame + "-----Boltname-----" + boltname);


            }
            else System.out.println("\n\n\t\t********* JSON File does not exist in conf *********");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ExecMappingconfig;
    }

    public static void main(String[] args) {
        System.out.println(JsonFIleReader.getJsonConfig("/Users/anshushukla/Downloads/scheduler/src/main/java/sampleJSON.json","test","Third"));
        System.out.println(JsonFIleReader.getJsonThreadCount("/Users/anshushukla/Downloads/scheduler/src/main/java/sampleJSON.json","test","Third"));
    }
}