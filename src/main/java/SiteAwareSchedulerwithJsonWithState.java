
    package main.java;

    import org.apache.storm.generated.Bolt;
    import org.apache.storm.generated.SpoutSpec;
    import org.apache.storm.generated.StormTopology;
    import org.apache.storm.scheduler.*;


    import org.json.simple.parser.JSONParser;

    import java.util.*;

    //running 9_3_30 before that
    public class SiteAwareSchedulerwithJsonWithState implements IScheduler {

        private static final String SITE = "site"; //used only while caching supervisor details
    //    private static final String THREADS = "threads";
    String jsonfilepath = "/data/tetc/apache-storm-0.9.4-ForScheduling/conf/inputTopoConfig.json";

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
                System.out.println("\n\nboltName_Set_FromConf-" + boltName_Set_FromConf);
                System.out.println("workerslot_Set_FromConf-" + workerslot_Set_FromConf);
                //TODO:check bofore running
//                StateFromConf.createStateFromConf(boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf, boltName_IntegerMap, slotName_IntegerMap);
                System.out.println("\n\t\t\t\t--Conf state done--");


                Map<String, Integer> test_boltname_NumberPair = new HashMap<>();
                Map<WorkerSlot,Integer> test_workeSlot_NumberPair = new HashMap<>();
                int[][] CurrentexecToboltNameMatrix=null;
                System.out.println("UtilityFunction_workeSlot_NumberPair-"+test_workeSlot_NumberPair);
                System.out.println("UtilityFunction_boltname_NumberPair-"+test_boltname_NumberPair);
                System.out.println("CurrentexecToboltNameMatrix-"+CurrentexecToboltNameMatrix);

                needsSchedulingFlag = UtilityFunction.setFlagforScheduling(t, cluster, jsonfilepath, needsSchedulingFlagBolt, needsSchedulingFlagSpout,vm_Name_supIDMap);


                //
                if (needsSchedulingFlag == 1) {

//                    StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
                    Map<String, SupervisorDetails> supervisors = new HashMap<>();
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
//c2
//                    Map<String, String> vm_Name_supIDMap = new HashMap<>();
//                    vm_Name_supIDMap = StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
//                    System.out.println("vm_Name_supIDMap-\n" + vm_Name_supIDMap + "\n");

//                    for (TopologyDetails t : topologyDetails) {

                        StormTopology topology = t.getTopology();
                        String topoID = t.getId();
                        String topoName = t.getName();

                        Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
                        Map<String, Bolt> bolts = topology.get_bolts();
                        Map<String, SpoutSpec> spouts = topology.get_spouts();
//                        Set<String> boltName_Set_FromConf = new HashSet<>();
//                        Set<String> workerslot_Set_FromConf = new HashSet<>();
//                        List<String> FullMappingRes_conf = new ArrayList();
                        String site = null;
                        String slotID = null;
                        String threadCount = null;
                        Map<String, List<ExecutorDetails>> vmSlotExecMapping = new HashMap<String, List<ExecutorDetails>>();
                        for (String boltName : bolts.keySet()) {//get key and value in same loop
                            List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
                            Bolt bolt = bolts.get(boltName);
                            System.out.println("\n\n\nBolt name is -" + boltName + "-full bolt-" + bolt + "-bolt_get Common result-" + bolt.get_common());


//c1
//                            StateFromConf.createSetFromConf(jsonfilepath, topoName, boltName, vm_Name_supIDMap, boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf);//state from conf
                            String boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, boltName);
                            String boltMappingThreads = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, boltName);
//                        System.out.println("-boltMappingConfig-" + boltMappingConfig + "-boltMappingThreads-" + boltMappingThreads);

                            List<String> mappings = new ArrayList();
                            executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                            System.out.println("executors within this bolt- " + executors);
                            int NoOfexecutors;
                            int index = 0;
                            mappings = Arrays.asList(boltMappingConfig.split("/"));
//                        System.out.println("mappings -" + mappings);
                            Iterator mapping_itr = mappings.iterator();

                            //idea1:using first as key
                            //                    String KeyforMap=mappings.get(0).split(",")[0];
                            //                    vmSlotExecMapping.put(KeyforMap, executors);

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
                            }

                            //                    if (executors != null && executors.size() != totalExecconf) {
                            //                        System.out.println("**********Parallelism Hint and Conf value are not equal !!! Please see Map in topo for bolt -" + name + "*********EXITING****");
                            //
                            //                    }
                            else {

                                UtilityFunction.putExecListToboltnameMapping(boltName, executors, execToboltNameMapping);//check for null first
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
                                System.out.println("vmSlotExecMapping key/value set-" + vmSlotExecMapping);

                            }

                            //may need to open USE:syso
                            System.out.println("\n\ngetting putExecListToboltnamemapping-");
                            UtilityFunction.printExecListToboltnameMapping(execToboltNameMapping);
                        }

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
                        execToslotMapping = UtilityFunction.getCurrentExectoSlotMapping(cluster, topoID);//includes spout also


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
                                UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first
//                            execToslotMapping=UtilityFunction.removeSpout_CurrentExectoSlotMapping(spout_executors,execToslotMapping);
                            }
                        }
                        //printing current assignment
//                    execToslotMapping=UtilityFunction.removeSpout_CurrentExectoSlotMapping(spout_executors,execToslotMapping);//removing spout_executors from  mapping
                        System.out.println("Before calling Join Utility function arg passed -" + execToslotMapping);
                    //comment to avoid error
//                    CurrentexecToboltNameMatrix=UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping(execToslotMapping, execToboltNameMapping,test_boltname_NumberPair,test_workeSlot_NumberPair, currentexecToboltNameMap);


                } else {
                    System.out.println("\n\n\t\t\t\t-- Topo does not need scheduling--"+t.getName());
                }
            }
        }

    }


