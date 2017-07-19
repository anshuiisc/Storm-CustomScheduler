
package main.java;

import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.Component;

import java.util.*;


public class SiteAwareSchedulerwithJsonWithStateMatrixV1WithAcker implements IScheduler {

    private static final String SITE = "site"; //used only while caching supervisor details
    //    private static final String THREADS = "threads";
//    String jsonfilepath = "/data/tetc/apache-storm-0.9.4-ForScheduling/conf/inputTopoConfig.json";
    String jsonfilepath = "/home/anshu/storm/apache-storm-1.0.1/conf/inputTopoConfig.json";
//    String jsonfilepath ="/Users/anshushukla/Downloads/Storm/apache-storm-1.1.0/conf/inputTopoConfig.json";
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
        int needsSchedulingFlag = 0;




        Set<String> strings = topologies.getAllComponents().keySet();
        for(String s:strings){
            Map<String, Component> stringComponentMap = topologies.getAllComponents().get(s);
            Set<String> strings1 = stringComponentMap.keySet();
            for(String s1:strings1)
            System.out.println("TEST_getAllComponents:"+stringComponentMap.get(s1).toString()+","+stringComponentMap.get(s1));
        }

        for (TopologyDetails topo : topologyDetails) {

            // start
            Map<String, List<ExecutorDetails>> executorsByComponent = cluster.getNeedsSchedulingComponentToExecutors(topo);
            // Get a map of tag to components
             Map<String, ArrayList<String>> componentsByTag = new HashMap<String, ArrayList<String>>();
             UtilityFunction.populateComponentsByTagWithStormInternals(componentsByTag, executorsByComponent.keySet());
            System.out.println("TEST_componentsByTag:");
             for(String k:componentsByTag.keySet()){
                 System.out.println(k);
                 System.out.println(Arrays.toString(componentsByTag.get(k).toArray()));
             }

            //end

            //creating VM name and supID mapping
            Map<String, String> vm_Name_supIDMap = new HashMap<>();
            vm_Name_supIDMap = StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
            System.out.println("vm_Name_supIDMap-\n" + vm_Name_supIDMap + "\n");


            System.out.println("\n\t\t\t\t--Conf state creation started--");
            //create input sets from conf
            Set<String> boltName_Set_FromConf = new HashSet<>();
            Set<String> workerslot_Set_FromConf = new HashSet<>();
            List<String> FullMappingRes_conf = new ArrayList();

            StateFromConf.createSetFromConf(jsonfilepath, topo, vm_Name_supIDMap, boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf,componentsByTag);

            int row_size_fromConf = workerslot_Set_FromConf.size();
            int column_size_fromConf = boltName_Set_FromConf.size();
            HashMap<String, Integer> boltName_IntegerMap = new HashMap<>();
            HashMap<String, Integer> slotName_IntegerMap = new HashMap<>();
            HashMap<String, HashMap<String, Integer>> execToboltNameMap_Conf_State = new HashMap<>();

            StateFromConf.createStateFromConf(boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf, boltName_IntegerMap, slotName_IntegerMap, execToboltNameMap_Conf_State);

            System.out.println("execToboltNameMap_from_Conf-" + execToboltNameMap_Conf_State);
            System.out.println("slotName_IntegerMap" + "-" + slotName_IntegerMap + "\n-boltName_IntegerMap-" + boltName_IntegerMap + "\n-FullMappingRes_conf-" + FullMappingRes_conf);
            System.out.println("\n\t\t\t\t--Conf state done--");


            System.out.println("\n\nTest:Current state Start-----------------------------------------");

            HashMap<WorkerSlot, HashMap<String, Integer>> currentState_execToboltNameMap = new HashMap<>();
            Map<String, Integer> current_boltname_NumberPair = new HashMap<>();
            Map<WorkerSlot, Integer> current_workeSlot_NumberPair = new HashMap<>();
            int[][] currentexecToboltNameMatrix = new int[row_size_fromConf][column_size_fromConf];

            if(componentsByTag.get("untagged")==null)
                componentsByTag.put("untagged",new ArrayList<String>());

            Map<ExecutorDetails, String> _executorDetailsStringMapDummy = UtilityFunction.getcurrentExecListToboltname(topo, cluster,componentsByTag.get("untagged"));
            //_executorDetailsStringMapDummy will be empty after scheduling is done


//            if (_executorDetailsStringMapDummy.size() != 0)
//                this.executorDetailsStringMap = _executorDetailsStringMapDummy;


            //test
            if (_executorDetailsStringMapDummy.size() != 0) {
                System.out.println("log7-New code");
                if (this.executorDetailsStringMap != null) {
                    //failure case have returned something
                    System.out.println("log5-Inside else part");
                    for (ExecutorDetails e : _executorDetailsStringMapDummy.keySet()) {
                        String newBoltname = _executorDetailsStringMapDummy.get(e);
                        if (this.executorDetailsStringMap.containsKey(e)) {
                            this.executorDetailsStringMap.put(e, newBoltname);
                        } else {
                            this.executorDetailsStringMap.put(e, newBoltname);
                        }
                    }
                } else {//first iteration or fresh submission
                    System.out.println("log6-Inside else part");
                    this.executorDetailsStringMap = _executorDetailsStringMapDummy;
                }
            }
            //

            System.out.println("executorDetailsStringMap inside main-" + this.executorDetailsStringMap + "\n");
            if (executorDetailsStringMap != null) {
                currentexecToboltNameMatrix = UtilityFunction.createCurrentStateMatrix(topo, cluster, current_boltname_NumberPair, current_workeSlot_NumberPair, row_size_fromConf, column_size_fromConf, this.executorDetailsStringMap, currentState_execToboltNameMap);

            }
            System.out.println("CurrentexecToboltNameMatrix-" + Arrays.deepToString(currentexecToboltNameMatrix));


            System.out.println("currentexecToboltNameMap inside main function -" + currentState_execToboltNameMap + "\n\n");
            System.out.println("UtilityFunction_workeSlot_NumberPair-" + current_workeSlot_NumberPair + "\n\n");
            System.out.println("UtilityFunction_boltname_NumberPair-" + current_boltname_NumberPair + "\n\n");

            System.out.println("CurrentexecToboltNameMatrix-" + Arrays.deepToString(currentexecToboltNameMatrix));
            System.out.println("\t\t\t\tTest:Current state End------------------------------------\n\n");


            Map<String, SupervisorDetails> supervisors = new HashMap<>();
//            StormTopology topology = topo.getTopology();
//            String topoName = topo.getName();
//            Map<String, SpoutSpec> spouts = topology.get_spouts();
            //setting supervisor data
            //logging: getting supervisor names
            {
                for (String s : cluster.getSupervisors().keySet()) {
                    System.out.println("supervisor names are -" + s);
            }
            }
            //

            System.out.println("TEST:caching supervisors indexed by their sites in a hash map...");
            for (SupervisorDetails s : cluster.getSupervisors().values()) {
                //System.out.println("TEST:supervisor name-" + s);
                Map<String, String> metadata = (Map<String, String>) s.getSchedulerMeta();
                if (metadata.get(SITE) != null) {
                    System.out.println("TEST: checking if metadata is set on this supervisor....");
                    supervisors.put(metadata.get(SITE), s);
                    System.out.println("TEST:Value for this supervisor-" + metadata.get(SITE));
            }
                System.out.println(s.getAllPorts() + "\n");
            }

            //schedule bolt using hashmap
            ScheduleFromMapDiff.findMatrixDiffandSchedule(currentState_execToboltNameMap, execToboltNameMap_Conf_State, vm_Name_supIDMap, supervisors, cluster, topo);


        }
    }

}


