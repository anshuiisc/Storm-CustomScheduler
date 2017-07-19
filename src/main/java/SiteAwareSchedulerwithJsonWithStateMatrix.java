
    package main.java;

    import org.apache.storm.generated.SpoutSpec;
    import org.apache.storm.generated.StormTopology;
    import org.apache.storm.scheduler.*;

    import java.util.*;

    //running 9_3_30 before that
    public class SiteAwareSchedulerwithJsonWithStateMatrix implements IScheduler {

        private static final String SITE = "site"; //used only while caching supervisor details
    //    private static final String THREADS = "threads";
    String jsonfilepath = "/data/tetc/apache-storm-0.9.4-ForScheduling/conf/inputTopoConfig.json";
        Map<ExecutorDetails, String> executorDetailsStringMap;

        @Override
        public void prepare(Map conf) {
        }

        @Override
        public void schedule(Topologies topologies, Cluster cluster) {
            System.out.println("=======================================TEST:running====================================");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();  //get supervisor details of cluster
            int needsSchedulingFlagBolt = 0;
            int needsSchedulingFlagSpout = 0;
            int needsSchedulingFlag=0;

            for (TopologyDetails t : topologyDetails) {

                Map<String, String> vm_Name_supIDMap = new HashMap<>();
                vm_Name_supIDMap = StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
                System.out.println("vm_Name_supIDMap-\n" + vm_Name_supIDMap + "\n");
                Set<String> boltName_Set_FromConf = new HashSet<>();
                Set<String> workerslot_Set_FromConf = new HashSet<>();
                List<String> FullMappingRes_conf = new ArrayList();
                StateFromConf.createSetFromConf(jsonfilepath,t,vm_Name_supIDMap,boltName_Set_FromConf,workerslot_Set_FromConf,FullMappingRes_conf);

//  System.out.println("\n\nboltName_Set_FromConf-" + boltName_Set_FromConf);
//                System.out.println("workerslot_Set_FromConf-" + workerslot_Set_FromConf);

                int row_size_fromConf=workerslot_Set_FromConf.size();
                int column_size_fromConf=boltName_Set_FromConf.size();

                HashMap<String, Integer> boltName_IntegerMap = new HashMap<>();
                HashMap<String, Integer> slotName_IntegerMap = new HashMap<>();
                HashMap<String, HashMap<String, Integer>> execToboltNameMap_from_Conf = new HashMap<>();

                StateFromConf.createStateFromConf(boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf, boltName_IntegerMap, slotName_IntegerMap, execToboltNameMap_from_Conf);
                System.out.println("execToboltNameMap_from_Conf-" + execToboltNameMap_from_Conf);
                System.out.println("slotName_IntegerMap" + "-" + slotName_IntegerMap + "-" + boltName_IntegerMap + "-" + FullMappingRes_conf);

                System.out.println("\n\t\t\t\t--Conf state done--");


                System.out.println("\n\nTest:Start");
                Map<String, Integer> current_boltname_NumberPair = new HashMap<>();
                Map<WorkerSlot, Integer> current_workeSlot_NumberPair = new HashMap<>();

                int[][] currentexecToboltNameMatrix = new int[row_size_fromConf][column_size_fromConf];
                Map<ExecutorDetails, String> _executorDetailsStringMapDummy = UtilityFunction.getcurrentExecListToboltname(t, cluster);
                if(_executorDetailsStringMapDummy.size()!=0)
                    this.executorDetailsStringMap = _executorDetailsStringMapDummy;

                System.out.println("executorDetailsStringMap-"+ this.executorDetailsStringMap +"\n");
                //comment to avoid error
//                    if(executorDetailsStringMap!=null)
//                    currentexecToboltNameMatrix=UtilityFunction.createCurrentMatrixDemo(t,cluster,current_boltname_NumberPair,current_workeSlot_NumberPair,row_size_fromConf,column_size_fromConf, this.executorDetailsStringMap, currentexecToboltNameMap);
                System.out.println("CurrentexecToboltNameMatrix-" + Arrays.deepToString(currentexecToboltNameMatrix));

                System.out.println("UtilityFunction_workeSlot_NumberPair-" + current_workeSlot_NumberPair);
                System.out.println("UtilityFunction_boltname_NumberPair-" + current_boltname_NumberPair);
//                System.out.println("CurrentexecToboltNameMatrix-"+CurrentexecToboltNameMatrix);

                System.out.println("CurrentexecToboltNameMatrix-" + Arrays.deepToString(currentexecToboltNameMatrix));
                System.out.println("\t\t\t\tTest:End------------------------------------\n\n");

//comment to avoid error
//                UtilityFunction.findMatrixDiff(current_boltname_NumberPair,current_workeSlot_NumberPair,currentexecToboltNameMatrix,boltName_IntegerMap,slotName_IntegerMap,FullMappingRes_conf, vmSlotExecMapping);

                needsSchedulingFlag = UtilityFunction.setFlagforScheduling(t, cluster, jsonfilepath, needsSchedulingFlagBolt, needsSchedulingFlagSpout,vm_Name_supIDMap);


                if (needsSchedulingFlag == 1) {
                    Map<String, List<ExecutorDetails>> vmSlotExecMapping = new HashMap<String, List<ExecutorDetails>>();
                    Map<String, SupervisorDetails> supervisors = new HashMap<>();
                    StormTopology topology = t.getTopology();
                    String topoName = t.getName();
                    Map<String, SpoutSpec> spouts = topology.get_spouts();

                    UtilityFunction.createSlotToExeListmappingForScheduling(t, cluster, SITE, vmSlotExecMapping, topology, supervisors, topoName, jsonfilepath);
                    System.out.println("After commenting vmSlotExecMapping is -" + vmSlotExecMapping);


                        //code for scheduling bolts
                        for (String s : vmSlotExecMapping.keySet()) {
                            System.out.println("Bolt Schedling Started");
                            String vm_name = s.split("#")[0];
                            int port_number_from_Conf = Integer.parseInt(s.split("#")[1]);
//                        System.out.println("VM name is -" + vm_name+"port_number_from_Conf is-"+port_number_from_Conf);
                            SupervisorDetails supervisor = supervisors.get(vm_name);
                            List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);

                            if (vmSlotExecMapping.get(s) != null) {
                                if (!workerSlots.isEmpty()) {
                                    for (WorkerSlot w : workerSlots) {
                                        if (w.getPort() == port_number_from_Conf) {
                                            System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort() + "-port_number_from_Conf-" + port_number_from_Conf);
                                            cluster.assign(w, t.getId(), vmSlotExecMapping.get(s));
                                        }
                                    }
                                } else {
                                    System.out.println("No Worker slot is empty for this task-" + s);
                                }
                            }
                            System.out.println("\n\t\t-- Bolt Schedling Done --");
//                        //logging :getting deatils of worker slot
//                        for (WorkerSlot w : workerSlots) {
//                            System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort());
//                        }
                        }

                        Map<ExecutorDetails, WorkerSlot> execToslotMapping = new HashMap<ExecutorDetails, WorkerSlot>();
//                        execToslotMapping = UtilityFunction.getCurrentExectoSlotMapping(cluster, topoID);//includes spout also


                        List<ExecutorDetails> spout_executors = new ArrayList<>();
                        System.out.println("execToslotMapping-" + execToslotMapping);
                        for (String spoutName : spouts.keySet()) {
                            String site1 = null;
                            SpoutSpec spout = spouts.get(spoutName);
//                        System.out.println("---Reading JSON file---");
                            String spoutMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, spoutName);
                            String boltMappingThreads = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, spoutName);
                            //CHECK:No split to this config
                            if (spoutMappingConfig != null && supervisors.get(spoutMappingConfig) != null) {
                                site1 = spoutMappingConfig;
                                System.out.println("TEST:inside topology loop for spout-" + site1);
                            }
                            SupervisorDetails supervisor = supervisors.get(site1);
                            List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
                            spout_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(spoutName);
                            if (!workerSlots.isEmpty() && spout_executors != null) {
                                System.out.println("Going to assign spout" + workerSlots.get(0) + "-" + spout_executors);
                                cluster.assign(workerSlots.get(0), t.getId(), spout_executors);
//                                UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first

                            }
                        }

                        System.out.println("Before calling Join Utility function arg passed -" + execToslotMapping);
//                    currentexecToboltNameMatrix=UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping(execToslotMapping, execToboltNameMapping,current_boltname_NumberPair,current_workeSlot_NumberPair);


                } else {
                    System.out.println("\n\n\t\t\t\t-- Topo does not need scheduling--"+t.getName());
                }
            }
        }

    }


