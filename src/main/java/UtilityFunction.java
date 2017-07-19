    package main.java;

    import org.apache.storm.generated.Bolt;
    import org.apache.storm.generated.SpoutSpec;
    import org.apache.storm.generated.StormTopology;
    import org.apache.storm.scheduler.*;
    import org.json.simple.parser.JSONParser;

    import java.util.*;

    /**
     * Created by anshushukla on 13/03/16.
     */
    public class UtilityFunction {

        public static Map<ExecutorDetails, WorkerSlot> getCurrentExectoSlotMapping(Cluster cluster, String topoID)
        {
            SchedulerAssignment sa =  cluster.getAssignmentById(topoID);

            Map<ExecutorDetails, WorkerSlot> execToslotMapping = new HashMap<>();
            System.out.println("SchedulerAssignment-"+sa);
            if(sa!=null) {
                execToslotMapping = sa.getExecutorToSlot();
                System.out.println("Current state Mapping--\n\n");
                for (ExecutorDetails key : execToslotMapping.keySet()) {
                    System.out.println(key + " - " + execToslotMapping.get(key).getPort());

                }
            }


    //        Map<WorkerSlot,List<ExecutorDetails>>  slotToexecMapping  = new HashMap<>();
    //        for (ExecutorDetails key : execToslotMapping.keySet()) {
    //
    //            WorkerSlot w=execToslotMapping.get(key);
    //            System.out.println(key + " - " + w);
    //            if(slotToexecMapping.containsKey(w)){
    //                //key exists so append
    //                System.out.println("value already entered");
    //
    //                slotToexecMapping.put(w,)
    //            }else{
    //                //key not exists
    //                slotToexecMapping.put(w,);
    //
    //            }
    //        }

            return execToslotMapping;
        }

        public static void putExecListToboltnameMapping(String boltName, List<ExecutorDetails> executors, Map<ExecutorDetails, String> execToboltNameMapping){

            System.out.println("putExecListToboltnamemapping----");
            if(executors!=null){
                for (ExecutorDetails e:executors){
                    execToboltNameMapping.put(e,boltName);
                    System.out.println(e+"-"+boltName);
                }
            }
        }

//just for reading no action
public static void printExecListToboltnameMapping(Map<ExecutorDetails, String> execToboltNameMapping) {
            for (ExecutorDetails key : execToboltNameMapping.keySet()) {
                System.out.println(key + " - " + execToboltNameMapping.get(key));

            }
        }

        public static int[][] joinExecToboltNameAndExecToslotMapping(Map<ExecutorDetails, WorkerSlot> execToslotMapping, Map<ExecutorDetails, String> execToboltNameMapping, Map<String, Integer> test_boltname_NumberPair, Map<WorkerSlot, Integer> test_workeSlot_NumberPair, HashMap<WorkerSlot, HashMap<String, Integer>> currentState_execToboltNameMap)
        {
//Actual map holding current state-            currentState_execToboltNameMap

            System.out.println("Current state JOINED Mapping--\n\n");
            //get list of ports as rows ,get list of boltnames as columns,entry is size of exec_list from getCurrentExectoSlotMapping output

            String boltName=null;
            WorkerSlot w=null;
            Set<WorkerSlot> test_SlotSet = new HashSet<WorkerSlot>();
                for (ExecutorDetails key : execToslotMapping.keySet()) {
                    test_SlotSet.add(execToslotMapping.get(key));
                }
            test_workeSlot_NumberPair.putAll(UtilityFunction.WorkerSlotsetToSortedIndexedList(test_SlotSet));

            Set<String> test_NameSet = new HashSet<String>();
                for (ExecutorDetails key : execToboltNameMapping.keySet()) {
                    test_NameSet.add(execToboltNameMapping.get(key));
                }
            test_boltname_NumberPair.putAll(UtilityFunction.StringsetToSortedIndexedMap(test_NameSet));
//            System.out.println("UtilityFunction_workeSlot_NumberPair-"+test_workeSlot_NumberPair);
//            System.out.println("UtilityFunction_boltname_NumberPair-"+test_boltname_NumberPair);

            int[][] CurrentexecToboltNameMatrix=new int[test_workeSlot_NumberPair.size()][test_boltname_NumberPair.size()];
                    for (ExecutorDetails exec : execToslotMapping.keySet()) {

                        WorkerSlot workerSlot = execToslotMapping.get(exec);
                        String _boltname = execToboltNameMapping.get(exec);//TODO: check error giving null entry on failure

                        //storing state to matrix (Not used actually for now)
//                        System.out.println("workerSlot-"+workerSlot);
                        int row_number=test_workeSlot_NumberPair.get(workerSlot);
//                        System.out.println("Exception-"+s+"exec"+exec);
                        if (_boltname != null) {
                            int column_number = test_boltname_NumberPair.get(_boltname);
//                        execToboltNameMatrix[row_number][column_number]+=entry;
                            CurrentexecToboltNameMatrix[row_number][column_number] += 1;
                            System.out.println(exec + " - " + workerSlot.getPort() + "-Boltname-" + _boltname + "-row-" + row_number + "-column-" + column_number);
                        }

                        //storing state to MAP ,used further for scheduling
                        if (currentState_execToboltNameMap != null || currentState_execToboltNameMap.size() != 0) {
                            //
                            if (currentState_execToboltNameMap.containsKey(workerSlot)) {
                                if (currentState_execToboltNameMap.get(workerSlot).containsKey(_boltname)) {
                                    int prev_entry = currentState_execToboltNameMap.get(workerSlot).get(_boltname);
                                    currentState_execToboltNameMap.get(workerSlot).put(_boltname, prev_entry + 1);
                                } else {
                                    currentState_execToboltNameMap.get(workerSlot).put(_boltname, 1);
                                }
                            } else {
                                HashMap<String, Integer> temp = new HashMap<>();
                                temp.put(_boltname, 1);
                                currentState_execToboltNameMap.put(workerSlot, temp);
                            }
                        }
                    }
            System.out.println("currentexecToboltNameMap in join -" + currentState_execToboltNameMap);
            System.out.println("printing a current 2-D array-"+Arrays.deepToString(CurrentexecToboltNameMatrix));

            //actual state is updated by reference at currentState_execToboltNameMap Matrix is not used
            return CurrentexecToboltNameMatrix;

            }



        public  static int setFlagforScheduling(TopologyDetails t_name, Cluster cluster, String jsonfilepath, int needsSchedulingFlagBolt, int needsSchedulingFlagSpout, Map<String, String> vm_Name_supIDMap){

//            for (TopologyDetails t_name : topologyDetails) {
                StormTopology topology = t_name.getTopology();
                String topoName = t_name.getName();
                System.out.println("\n\n\t\t\t\t--Checking topo needs scheduling---" + topoName);

                //idea2:--- New CODE Checking topo needs scheduling --

                Map<String, Bolt> bolts = topology.get_bolts();
                Map<String, SpoutSpec> spouts = topology.get_spouts();
                List<ExecutorDetails> executors = new ArrayList<>();


                for (String boltName : bolts.keySet()) {
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t_name).get(boltName);
                    String thread_count_in_conf = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, boltName);
                    System.out.println("executors within bolt- " + boltName + "-" + executors);

//                    StateFromConf.createSetFromConf(jsonfilepath, topoName, boltName, vm_Name_supIDMap, boltName_Set_FromConf, workerslot_Set_FromConf, fullMappingRes_conf);//state from conf

                    if (executors != null) {

                        //while doing rebalance/new submission thread_count_in_conf = = executor.size for that bolt
                        System.out.println("CHECKING: Rebalance/New submission thread_count_in_conf = = executor.size for that bolt");
                        System.out.println("\t\texecutor list size--" + executors.size() + "-thread_count_in_conf-" + thread_count_in_conf);
                        if (executors.size() != Integer.parseInt(thread_count_in_conf)) {//checking conf_thread --> (topo_exec passed/Rebalance_exec_passed)
                            System.out.println("\n\n\t\t\t\t****FAILED/UNEQUAL:EITHER some executors have failed OR Conf entry is wrong For-" + topoName + "---" + boltName + "******");
                            System.out.println("\n\n\t\t\t\t**No logic to handle Re-Scheduling----\n\n\t\tEXITING***");
                            //No logic to handle Re-Scheduling
                            needsSchedulingFlagBolt = 0;
                            break;
                        } else {
                            needsSchedulingFlagBolt = 1;
                            System.out.println("\t\t\t\t*****Rebalance/New submission--Toponame********" + topoName + "****BoltName****" + boltName+"\n\n");
                        }

                    }
                }


                for (String spoutName : spouts.keySet()) {
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t_name).get(spoutName);
                    System.out.println("executors within spout- " + executors);
                    if (executors != null && needsSchedulingFlagBolt == 1)//Flag checking: if condition for all bolts are fine the inly go for spout
                    {
                        needsSchedulingFlagSpout = 1;
                        System.out.println("\t\t\t\t*****Rebalance/New submission--Toponame********" + topoName + "****SpoutName****" + spoutName+"\n\n");
                    }

                }


                System.out.println("\t\tFLAGS:needsSchedulingFlagBolt-" + needsSchedulingFlagBolt + "-needsSchedulingFlagSpout-" + needsSchedulingFlagSpout+"\n");
//            }
            if(needsSchedulingFlagBolt==1 || needsSchedulingFlagSpout==1)
                return 1;
            else return 0;


        }




        public static Map<WorkerSlot, Integer> WorkerSlotsetToSortedIndexedList(Set<WorkerSlot> _test_SlotSet){
            List<WorkerSlot> test_Slotlist = new ArrayList<WorkerSlot>(_test_SlotSet);
            Map<WorkerSlot,Integer> _test_workeSlot_NumberPair = new HashMap<>();
            int testcount_workeSlot_NumberPair=0;
//            System.out.println("old list-" + test_Slotlist);
            Collections.sort(test_Slotlist, new Comparator<WorkerSlot>() {
                public int compare(WorkerSlot idx1, WorkerSlot idx2) {
                    return Double.compare(idx1.getPort(), idx2.getPort());
                }
            });
//            System.out.println("sorted list-" + test_Slotlist);
            for (WorkerSlot wk : test_Slotlist) {
                _test_workeSlot_NumberPair.put(wk, testcount_workeSlot_NumberPair);
                testcount_workeSlot_NumberPair+=1;
            }
            return _test_workeSlot_NumberPair;
        }


        public static Map<String, Integer> WorkerSlotStringsetToSortedIndexedMap(Set<String> _test_SlotSet) {

            List<String> test_Slotlist = new ArrayList<String>(_test_SlotSet);
            Map<String,Integer> _test_workeSlot_NumberPair = new HashMap<>();
            int testcount_workeSlot_NumberPair=0;
//            System.out.println("old list-" + test_Slotlist);
            Collections.sort(test_Slotlist, new Comparator<String>() {
                public int compare(String idx1, String idx2) {
                    Double d1=Double.parseDouble(idx1.split(":")[1]);
                    Double d2=Double.parseDouble(idx2.split(":")[1]);
                    return Double.compare(d1,d2);
                }
            });
//            System.out.println("sorted list-" + test_Slotlist);
            for (String wk : test_Slotlist) {
                _test_workeSlot_NumberPair.put(wk, testcount_workeSlot_NumberPair);
                testcount_workeSlot_NumberPair+=1;
            }
            return _test_workeSlot_NumberPair;
        }

        public static HashMap<String, Integer> StringsetToSortedIndexedMap(Set<String> _test_NameSet) {
            List<String> test_Namelist = new ArrayList<String>(_test_NameSet);
            HashMap<String, Integer> _test_boltname_NumberPair = new HashMap<>();
            int testcount=0;
//            System.out.println("old name list-" + test_Namelist);
            Collections.sort(test_Namelist);
//            System.out.println("sorted name list-" + test_Namelist);
            for (String wk : test_Namelist) {
                _test_boltname_NumberPair.put(wk, testcount);
                testcount+=1;
            }
        return _test_boltname_NumberPair;
        }

        public static Map<ExecutorDetails, String> getcurrentExecListToboltname(TopologyDetails t, Cluster cluster,ArrayList<String> componentsByTag) {
            StormTopology topology = t.getTopology();
            String topoID = t.getId();
            String topoName = t.getName();
            Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();


            List<ExecutorDetails> bolt_executors = new ArrayList<ExecutorDetails>();
            List<ExecutorDetails> spout_executors = new ArrayList<>();



            for(String s:componentsByTag){
                System.out.println("TEST_ACKER_Name:"+s);
                bolt_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(s);
                System.out.println("acker executors within this bolt- " + bolt_executors);
                System.out.println("acker_executors-"+bolt_executors);
                if(bolt_executors!=null) {
                    UtilityFunction.putExecListToboltnameMapping(s, bolt_executors, execToboltNameMapping);//check for null first
                }
            }

            for (String boltName : bolts.keySet()) {
                //get key and value in same loop
                System.out.println("TEST_boltName"+boltName);
                bolt_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                System.out.println("executors within this bolt- " + bolt_executors);
//                Bolt bolt = bolts.get(boltName);
                System.out.println("bolt_executors-"+bolt_executors);
                if(bolt_executors!=null) {
                    UtilityFunction.putExecListToboltnameMapping(boltName, bolt_executors, execToboltNameMapping);//check for null first
                }
            }


            for (String spoutName : spouts.keySet()) {
                System.out.println("TEST_spoutName"+spoutName);
                SpoutSpec spout = spouts.get(spoutName);
                spout_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(spoutName);
                System.out.println("spout_executors-"+spout_executors);
                if(spout_executors != null) {
                    UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first
                }
            }

            System.out.println("\n\nFull printExecListToboltnameMapping-");
            UtilityFunction.printExecListToboltnameMapping(execToboltNameMapping);

            System.out.println("execToboltNameMapping size-"+execToboltNameMapping.size());
            return execToboltNameMapping;
        }

        public static Map<ExecutorDetails, String> getcurrentExecListToboltname(TopologyDetails t, Cluster cluster) {
            StormTopology topology = t.getTopology();
            String topoID = t.getId();
            String topoName = t.getName();
            Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();


            List<ExecutorDetails> bolt_executors = new ArrayList<ExecutorDetails>();
            List<ExecutorDetails> spout_executors = new ArrayList<>();


            for (String boltName : bolts.keySet()) {
                //get key and value in same loop
                System.out.println("TEST_boltName"+boltName);
                bolt_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                System.out.println("executors within this bolt- " + bolt_executors);
                Bolt bolt = bolts.get(boltName);
                System.out.println("bolt_executors-"+bolt_executors);
                if(bolt_executors!=null) {
                    UtilityFunction.putExecListToboltnameMapping(boltName, bolt_executors, execToboltNameMapping);//check for null first
                }
            }


            for (String spoutName : spouts.keySet()) {
                System.out.println("TEST_spoutName"+spoutName);
                SpoutSpec spout = spouts.get(spoutName);
                spout_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(spoutName);
                System.out.println("spout_executors-"+spout_executors);
                if(spout_executors != null) {
                    UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first
                }
            }

            System.out.println("\n\nFull printExecListToboltnameMapping-");
            UtilityFunction.printExecListToboltnameMapping(execToboltNameMapping);

            System.out.println("execToboltNameMapping size-"+execToboltNameMapping.size());
            return execToboltNameMapping;
        }


        public static int[][] createCurrentStateMatrix(TopologyDetails t, Cluster cluster, Map<String, Integer> current_boltname_NumberPair, Map<WorkerSlot, Integer> current_workeSlot_NumberPair, int row_size_fromConf, int column_size_fromConf, Map<ExecutorDetails, String> executorDetailsStringMap, HashMap<WorkerSlot, HashMap<String, Integer>> currentState_execToboltNameMap) {

            String topoID = t.getId();

            Map<ExecutorDetails, WorkerSlot> execToslotMapping = new HashMap<ExecutorDetails, WorkerSlot>();
            execToslotMapping = UtilityFunction.getCurrentExectoSlotMapping(cluster, topoID);//includes spout also

            int[][] CurrentexecToboltNameMatrix=new int[row_size_fromConf][column_size_fromConf];

            System.out.println("Before calling Join Utility function arg passed -" + execToslotMapping);
            if(execToslotMapping.size()!=0 && execToslotMapping!=null) {
                CurrentexecToboltNameMatrix = UtilityFunction.joinExecToboltNameAndExecToslotMapping(execToslotMapping, executorDetailsStringMap, current_boltname_NumberPair, current_workeSlot_NumberPair, currentState_execToboltNameMap);
            }
            else{
                System.out.println("UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping  is not called");
            }
        return  CurrentexecToboltNameMatrix;

        }

        public static  void populateComponentsByTagWithStormInternals(
                Map<String, ArrayList<String>> componentsByTag,
                Set<String> components) {
            // Storm uses some internal components, like __acker.
            // These components are topology-agnostic and are therefore not accessible through a StormTopology object.
            // While a bit hacky, this is a way to make sure that we schedule those components along with our topology ones:
            // we treat these internal components as regular untagged components and add them to the componentsByTag map.

            for (String componentID : components) {
                if (componentID.startsWith("__")) {
                    if (componentsByTag.containsKey("untagged")) {
                        // If we've already seen untagged components, then just add the component to the existing ArrayList.
                        componentsByTag.get("untagged").add(componentID);
                    } else {
                        // If this is the first untagged component we see, then create a new ArrayList,
                        // add the current component, and populate the map's untagged entry with it.
                        ArrayList<String> newComponentList = new ArrayList<String>();
                        newComponentList.add(componentID);
                        componentsByTag.put("untagged", newComponentList);
                    }
                }
            }
        }


        public static void createSlotToExeListmappingForScheduling(TopologyDetails t, Cluster cluster, String SITE, Map<String, List<ExecutorDetails>> vmSlotExecMapping, StormTopology topology, Map<String, SupervisorDetails> supervisors, String topoName, String jsonfilepath) {
            {
//                        Map<String, SupervisorDetails> supervisors = new HashMap<>();
                JSONParser parser = new JSONParser();
                //logging: getting supervisor names
                {
                    for (String s : cluster.getSupervisors().keySet()) {
                        System.out.println("supervisor names are -" + s);
                    }
                }
                //

                System.out.println("TEST:caching supervisors indexed by their sites in a hash map...");
                for (SupervisorDetails s : cluster.getSupervisors().values()) {
//                    System.out.println("TEST:supervisor name-" + s);
                    Map<String, String> metadata = (Map<String, String>) s.getSchedulerMeta();
                    if (metadata.get(SITE) != null) {
                        System.out.println("TEST: checking if metadata is set on this supervisor....");
                        supervisors.put(metadata.get(SITE), s);
                        System.out.println("TEST:Value for this supervisor-" + metadata.get(SITE));
                    }
                    System.out.println(s.getAllPorts() + "\n");
                }

                topology = t.getTopology();
                String topoID = t.getId();
                topoName = t.getName();

                Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
                Map<String, Bolt> bolts = topology.get_bolts();
                for (String boltName : bolts.keySet()) {//get key and value in same loop
                    List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
                    Bolt bolt = bolts.get(boltName);
                    System.out.println("\n\n\nBolt name is -" + boltName + "-full bolt-" + bolt + "-bolt_get Common result-" + bolt.get_common());

                    String boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, boltName);
                    String boltMappingThreads = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, boltName);

                    List<String> mappings = new ArrayList();
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                    System.out.println("executors within this bolt- " + executors);
                    int NoOfexecutors;
                    int index = 0;
                    mappings = Arrays.asList(boltMappingConfig.split("/"));
//                        System.out.println("mappings -" + mappings);
                    Iterator mapping_itr = mappings.iterator();


                    int curentBoltCount = Integer.parseInt(boltMappingThreads); //conf thread count here
                    int totalExecconf = 0;
                    for (String param : mappings) {
                        totalExecconf += Integer.parseInt(param.split(",")[1]);
                    }
                    System.out.println("checking JSON_total_thread ---> JSON_conf_thread_Mapping......... \n");
                    System.out.println("curentBoltThreadCount-" + curentBoltCount + "-totalExecconf-" + totalExecconf);
                    if (curentBoltCount != totalExecconf) {

                        System.out.println("\n\n\n\t\t**********Parallelism Hint and Conf value are not equal !!! Please see Map in topo for bolt -" + boltName);
                        System.out.println("\t\t*********Please kill topology****");
                        System.out.println("\t\ttopology name----" + t.getName());
                        System.out.println("\t\t*************EXITING**************\n\n\n\n");
                    } else {

//                                UtilityFunction.putExecListToboltnameMapping(boltName, executors, execToboltNameMapping);//check for null first
                        while (mapping_itr.hasNext() && executors != null) {
                            String s = (String) mapping_itr.next();
                            String KeyforMap = s.split(",")[0];
                            NoOfexecutors = Integer.parseInt(s.split(",")[1]);
//                                System.out.println("KeyforMap -" + KeyforMap + "- index -" + index + "- NoOfexecutors -" + NoOfexecutors);

                            //logic replacing sublist concept

                            List<ExecutorDetails> executorDetailses = new ArrayList(executors.subList(index, index + NoOfexecutors));
//                                System.out.println("inside iterator loop - " + KeyforMap + "-----" + executorDetailses);
                            if (vmSlotExecMapping.containsKey(KeyforMap)) {
//                                    System.out.println("ALready key inserted -" + KeyforMap);
                                List<ExecutorDetails> prevList = vmSlotExecMapping.get(KeyforMap);
                                prevList.addAll(executorDetailses);
//                                    System.out.println("after adding -" + prevList);
                            } else {
                                vmSlotExecMapping.put(KeyforMap, executorDetailses);
                            }
                            index += NoOfexecutors;
                        }
                        System.out.println("vmSlotExecMapping key/value set for scheduling from conf-" + vmSlotExecMapping);

                    }

                }
            }



        }


    }
