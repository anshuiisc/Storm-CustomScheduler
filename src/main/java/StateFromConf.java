    package main.java;

    import org.apache.storm.generated.Bolt;
    import org.apache.storm.generated.SpoutSpec;
    import org.apache.storm.generated.StormTopology;
    import org.apache.storm.scheduler.Cluster;
    import org.apache.storm.scheduler.SupervisorDetails;
    import org.apache.storm.scheduler.TopologyDetails;

    import java.util.*;

    /**
    * Created by anshushukla on 15/03/16.
    */
    public class StateFromConf {



    //        createStateFromConf
    public static void createSetFromConf(String jsonfilepath, TopologyDetails t_name, Map<String, String> vm_Name_supIDMap, Set<String> boltName_Set_FromConf, Set<String> workerslot_Set_FromConf, List fullMappingRes_conf)
    {
    //sample-            boltMappingConfig-orion1#6701,2/orion3#6718,1/orion3#6717,1/orion1#6702

        StormTopology topology = t_name.getTopology();
        String topoName = t_name.getName();
//        System.out.println("\n\n\t\t\t\t--Checking topo needs scheduling---" + topoName);
        Map<String, Bolt> bolts = topology.get_bolts();
        Map<String, SpoutSpec> spouts = topology.get_spouts();

        for (String boltName : bolts.keySet()) {
            String _boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, boltName);
            System.out.println("\n\n\t\t**********CONF state**********");
            System.out.println("Test:boltMappingConfig-" + _boltMappingConfig);
            String[] boltMappingConfig_list = _boltMappingConfig.split("/");
            for (String boltMappingConfig_list_val : boltMappingConfig_list) {
                int entry = Integer.parseInt(boltMappingConfig_list_val.split(",")[1]);
                String vm_NameFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[0];
                String vm_PortFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[1];
                String _wrkrSlot = vm_Name_supIDMap.get(vm_NameFromConf) + ":" + vm_PortFromConf;
                String res = _wrkrSlot + "," + boltName + "," + entry;
                System.out.println("\t\tcreateStateFromConf-" + res);

                boltName_Set_FromConf.add(boltName);
                workerslot_Set_FromConf.add(_wrkrSlot);
                fullMappingRes_conf.add(res);
            }
        }

        for (String spoutsName : spouts.keySet()) {
            String _boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, spoutsName);
            System.out.println("\n\n\t\t**********CONF state**********");
            System.out.println("Test:boltMappingConfig spout- " + _boltMappingConfig);
            String[] boltMappingConfig_list = _boltMappingConfig.split("/");
            for (String boltMappingConfig_list_val : boltMappingConfig_list) {
                int entry = Integer.parseInt(boltMappingConfig_list_val.split(",")[1]);
                String vm_NameFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[0];
                String vm_PortFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[1];
                String _wrkrSlot = vm_Name_supIDMap.get(vm_NameFromConf) + ":" + vm_PortFromConf;
                String res = _wrkrSlot + "," + spoutsName + "," + entry;
                System.out.println("\t\tcreateStateFromConf-" + res);

                boltName_Set_FromConf.add(spoutsName);
                workerslot_Set_FromConf.add(_wrkrSlot);
                fullMappingRes_conf.add(res);
            }
        }

    }



        //        createStateFromConf with componentsByTag
        public static void createSetFromConf(String jsonfilepath, TopologyDetails t_name, Map<String, String> vm_Name_supIDMap, Set<String> boltName_Set_FromConf, Set<String> workerslot_Set_FromConf, List fullMappingRes_conf,Map<String, ArrayList<String>> componentsByTag)
        {
            //sample-            boltMappingConfig-orion1#6701,2/orion3#6718,1/orion3#6717,1/orion1#6702

            StormTopology topology = t_name.getTopology();
            String topoName = t_name.getName();
//        System.out.println("\n\n\t\t\t\t--Checking topo needs scheduling---" + topoName);
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();

            for (String boltName : bolts.keySet()) {
                String _boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, boltName);
                System.out.println("\n\n\t\t**********CONF state**********");
                System.out.println("Test:boltMappingConfig-" + _boltMappingConfig);
                String[] boltMappingConfig_list = _boltMappingConfig.split("/");
                for (String boltMappingConfig_list_val : boltMappingConfig_list) {
                    int entry = Integer.parseInt(boltMappingConfig_list_val.split(",")[1]);
                    String vm_NameFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[0];
                    String vm_PortFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[1];
                    String _wrkrSlot = vm_Name_supIDMap.get(vm_NameFromConf) + ":" + vm_PortFromConf;
                    String res = _wrkrSlot + "," + boltName + "," + entry;
                    System.out.println("\t\tcreateStateFromConf-" + res);

                    boltName_Set_FromConf.add(boltName);
                    workerslot_Set_FromConf.add(_wrkrSlot);
                    fullMappingRes_conf.add(res);
                }
            }

            for (String spoutsName : spouts.keySet()) {
                String _boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, spoutsName);
                System.out.println("\n\n\t\t**********CONF state**********");
                System.out.println("Test:boltMappingConfig spout- " + _boltMappingConfig);
                String[] boltMappingConfig_list = _boltMappingConfig.split("/");
                for (String boltMappingConfig_list_val : boltMappingConfig_list) {
                    int entry = Integer.parseInt(boltMappingConfig_list_val.split(",")[1]);
                    String vm_NameFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[0];
                    String vm_PortFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[1];
                    String _wrkrSlot = vm_Name_supIDMap.get(vm_NameFromConf) + ":" + vm_PortFromConf;
                    String res = _wrkrSlot + "," + spoutsName + "," + entry;
                    System.out.println("\t\tcreateStateFromConf-" + res);

                    boltName_Set_FromConf.add(spoutsName);
                    workerslot_Set_FromConf.add(_wrkrSlot);
                    fullMappingRes_conf.add(res);
                }
            }

            if(componentsByTag.get("untagged")!=null){
                System.out.println("creating state for acker...");
                for(String ack:componentsByTag.get("untagged")){
                    String _boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, ack);
                    System.out.println("\n\n\t\t**********CONF state**********");
                    System.out.println("Test:boltMappingConfig-" + _boltMappingConfig);
                    String[] boltMappingConfig_list = _boltMappingConfig.split("/");
                    for (String boltMappingConfig_list_val : boltMappingConfig_list) {
                        int entry = Integer.parseInt(boltMappingConfig_list_val.split(",")[1]);
                        String vm_NameFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[0];
                        String vm_PortFromConf = boltMappingConfig_list_val.split(",")[0].split("#")[1];
                        String _wrkrSlot = vm_Name_supIDMap.get(vm_NameFromConf) + ":" + vm_PortFromConf;
                        String res = _wrkrSlot + "," + ack + "," + entry;
                        System.out.println("\t\tcreateStateFromConf-" + res);

                        boltName_Set_FromConf.add(ack);
                        workerslot_Set_FromConf.add(_wrkrSlot);
                        fullMappingRes_conf.add(res);
                    }
                }
            }

        }

        public static void createStateFromConf(Set<String> boltName_Set_FromConf, Set<String> workerslot_Set_FromConf, List<String> fullMappingRes_conf, Map<String, Integer> boltName_IntegerMap, Map<String, Integer> slotName_IntegerMap, HashMap<String, HashMap<String, Integer>> execToboltNameMap_from_Conf)
    {

        //convert sets --> sorted indexed list (Not used for actual state creation)
        boltName_IntegerMap.putAll(UtilityFunction.StringsetToSortedIndexedMap(boltName_Set_FromConf));
        slotName_IntegerMap.putAll(UtilityFunction.WorkerSlotStringsetToSortedIndexedMap(workerslot_Set_FromConf));
//        System.out.println("StateFromConf_boltName_IntegerMap-"+boltName_IntegerMap);
//        System.out.println("StateFromConf_slotName_IntegerMap-"+slotName_IntegerMap);
//        System.out.println("\nfullMappingRes_conf-"+fullMappingRes_conf);
        int[][] execToboltNameMatrix_from_Conf=new int[slotName_IntegerMap.size()][boltName_IntegerMap.size()];
        for(String s1:fullMappingRes_conf){
            String _workrSlotFromConf=(s1.split(",")[0]);
            String _boltFromConf=(s1.split(",")[1]);
            int _entryFromConf=Integer.parseInt(s1.split(",")[2]);
            execToboltNameMatrix_from_Conf[slotName_IntegerMap.get(_workrSlotFromConf)][boltName_IntegerMap.get(_boltFromConf)]=_entryFromConf;

            //convert sets --> sorted indexed map (used to create final state map and then diff )
            if (execToboltNameMap_from_Conf.containsKey(_workrSlotFromConf)) {
                execToboltNameMap_from_Conf.get(_workrSlotFromConf).put(_boltFromConf, _entryFromConf);

            } else {
                HashMap<String, Integer> temp = new HashMap<>();
                temp.put(_boltFromConf, _entryFromConf);
                execToboltNameMap_from_Conf.put(_workrSlotFromConf, temp);
            }
        }
//        System.out.println("execToboltNameMap_from_Conf-"+execToboltNameMap_from_Conf);
        System.out.println("printing a 2-D array for execToboltNameMatrix_from_Conf - "+Arrays.deepToString(execToboltNameMatrix_from_Conf));
    }



    public static Map<String, String> setVmNameSupervisorMapping(Cluster cluster, String SITE)
    {

        Map<String,String>  _vm_Name_supIDMap=new HashMap<>();
        Map<String,SupervisorDetails>  supID_Details_Mapping=cluster.getSupervisors();

        for(String _supID : supID_Details_Mapping.keySet()){
            SupervisorDetails   _Details=supID_Details_Mapping.get(_supID);
            Map<String, String> metadata = (Map<String, String>) _Details.getSchedulerMeta();
            if (metadata.get(SITE) != null) {
                String vm_name = metadata.get(SITE);
    //                System.out.println("TEST:-vm_name-" + vm_name+"-_supID-"+_supID);
                _vm_Name_supIDMap.put(vm_name,_supID);
            }
        }

        return _vm_Name_supIDMap;
    }
    }
