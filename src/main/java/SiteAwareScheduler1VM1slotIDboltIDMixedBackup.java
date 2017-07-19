package main.java;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;


public class SiteAwareScheduler1VM1slotIDboltIDMixedBackup implements IScheduler {

    private static final String SITE = "site";

    @Override
    public void prepare(Map conf) {}

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("TEST:running");
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();  //get supervisor details of cluster
        Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
        JSONParser parser = new JSONParser();


        //logging: getting supervisor names
        for (String s : cluster.getSupervisors().keySet())
        {
            System.out.println("supervisor names are -"+s);
        }
        //

        System.out.println("TEST:caching supervisors indexed by their sites in a hash map...");
        for (SupervisorDetails s : cluster.getSupervisors().values()) {
            System.out.println("TEST:supervisor name-"+s);
            Map<String, String> metadata = (Map<String, String>) s.getSchedulerMeta();
            if (metadata.get(SITE) != null) {
                System.out.println("TEST: checking if metadata is set on this supervisor....");
                supervisors.put((String) metadata.get(SITE), s);
                System.out.println("TEST:Value for this supervisor-" + (String) metadata.get(SITE));
            }
        }

        for (TopologyDetails t : topologyDetails) {
            //testing start
            Map<ExecutorDetails,String> EtoCmapping=cluster.getNeedsSchedulingExecutorToComponents(t);
            System.out.println("ExecutorToComponents  mapping-");
            for(ExecutorDetails e:EtoCmapping.keySet())
            {
                System.out.println("Component name--"+EtoCmapping.get(e));
                System.out.println("start task-"+e.getStartTask());
                System.out.println("end task-"+e.getEndTask());
            }
            //testing done

            if (!cluster.needsScheduling(t)) continue;
            StormTopology topology = t.getTopology();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            try {
                String site = null;
                String slotID = null;
                String threadCount = null;
//                List<ExecutorDetails> =new ArrayList<>();
                Map<String,List<ExecutorDetails>>  vmSlotExecMapping=new HashMap<String,List<ExecutorDetails>>();


                for (String name : bolts.keySet()) {
                    List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
                    Bolt bolt = bolts.get(name);

                    System.out.println("Bolt name is -" + name + "-full bolt-" + bolt + "-bolt_get Common result-" + bolt.get_common());

                    //parsing code from topology config
                    //sample

//                    Firstmap.put("site","uh#slot1,1,1/uh#slot2,1,1");

//                    Firstmap.put("F1","uh#slot1,1,1");
//                    Firstmap.put("F2","uh#slot2,1,1");
//
//                    Secondmap.put("S3","uh#slot2,1,1");
//                    Secondmap.put("S4","uh#slot3,1,1");
//
//                    Thirdmap.put("T5","uh#slot3,1,1");
//                    Thirdmap.put("T6","uh#slot1,1,1");


                    JSONObject conf = (JSONObject) parser.parse(bolt.get_common().get_json_conf());
                    List<String> mappings=new ArrayList();
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);

                    System.out.println("executors within this this bolt- "+ executors);

                    int NoOfexecutors;
                    int index=0;

                    mappings=Arrays.asList(((String) conf.get(SITE)).split("/"));
                    System.out.println("mappings -"+mappings);
                    Iterator itr = mappings.iterator();


                    String KeyforMap=mappings.get(0).split(",")[0];
                    vmSlotExecMapping.put(KeyforMap, executors);

//                   while(itr.hasNext()){
//                        String s= (String) itr.next();
//                        String KeyforMap=s.split(",")[0];
//                        NoOfexecutors= Integer.parseInt(s.split(",")[2]);
//
//                       System.out.println("KeyforMap -"+KeyforMap+"- index -"+index+"- NoOfexecutors -"+NoOfexecutors);
//
//
//                        List<ExecutorDetails> executorDetailses = executors.subList(index, index+NoOfexecutors);
//                        System.out.println("inside iterator loop - "+KeyforMap+"-----"+executorDetailses);
//                        vmSlotExecMapping.put(KeyforMap, executorDetailses);
//
////
////                        List<ExecutorDetails> executorDetailses = executors.subList(index, NoOfexecutors);
////                        if (vmSlotExecMapping.containsKey(KeyforMap)){
////                            List<ExecutorDetails> prevList=vmSlotExecMapping.get(KeyforMap);
////                                    prevList.addAll(executorDetailses);
////                            vmSlotExecMapping.put(KeyforMap, prevList);
////                        }
////                        else {
////                            vmSlotExecMapping.put(KeyforMap, executorDetailses);
////                        }
//                        index+=NoOfexecutors;
//
//                    }


                    System.out.println("vmSlotExecMapping keyset-"+vmSlotExecMapping.keySet());
//                    for (String exe:vmSlotExecMapping.keySet()){
//                        System.out.println(vmSlotExecMapping.get(exe));
//                    }

//                    System.out.println("vmSlotExecMapping valueset-"+vmSlotExecMapping.values());



//                    System.out.println("checking null in supervisor or/not-" + ((String) conf.get(SITE)).split(",")[0].split("#")[0]);
//
//                    if (conf.get(SITE) != null && supervisors.get(((String) conf.get(SITE)).split(",")[0].split("#")[0]) != null) {  //verify the site name on
//                        site = ((String) conf.get(SITE)).split(",")[0].split("#")[0];
//                        slotID = ((String) conf.get(SITE)).split(",")[0].split("#")[1];
//                        threadCount = ((String) conf.get(SITE)).split(",")[2];
//                        System.out.println("TEST:inside topology loop site for that bolt is-" + site + "-given config are -" + site + "-" + slotID + "-" + threadCount);
//                    }



//                    //logging :getting deatils of worker slot
//                    for (WorkerSlot w : workerSlots) {
//                        System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort());
//                    }
//                    for(int i=0;i<workerSlots.size();i++)
//                    {
//                        System.out.println("workerSlot.get() function " +workerSlots.get(i));
//                    }
                    //

//                    if(cluster.getNeedsSchedulingComponentToExecutors(t).get(name)!=null)
//                    executors.addAll(cluster.getNeedsSchedulingComponentToExecutors(t).get(name));


                }


                for(String s:vmSlotExecMapping.keySet()){
                    String vm_name=s.split("#")[0];
                    System.out.println("VM name is -"+vm_name);
                    SupervisorDetails supervisor = supervisors.get(vm_name);
                    List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
                    if (!workerSlots.isEmpty() && vmSlotExecMapping.get(s)!= null ) {
                        cluster.assign(workerSlots.get(0), t.getId(), vmSlotExecMapping.get(s));
                    }
                }


//                SupervisorDetails supervisor = supervisors.get(site);
//                List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);


                //logging :getting deatils of worker slot
//                for (WorkerSlot w : workerSlots) {
//                    System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort());
//                }
                //
                    //logging
//                    if (executors!=null){
//                    for(ExecutorDetails e : executors ) {
//                        System.out.println("executor list for that bolt- starttask-" + e.getStartTask()+"-Endtask"+e.getEndTask());
//                    }}
//                    System.out.println("Full executor list for that bolt after append - "+executors);
//                    //
//
//                    if (!workerSlots.isEmpty() && executors != null ) {
//                        cluster.assign(workerSlots.get(0), t.getId(), executors);
////                        //updated
////                        System.out.println("executor list before while loop- "+executors);
////                        int i=0;
////                        while(i<executors.size() ) {
////
////                            List<ExecutorDetails> executorDetailses = executors.subList(i, i+1);
////                            cluster.assign(workerSlots.get(i), t.getId(), executorDetailses);
////                            i+=1;
//////                            executors.remove(0);
//////                            workerSlots.remove(0);
////
////                            System.out.println("after removal - executorsBeing scheduled-"+executorDetailses+"-updated executor/workerSlot list list-"+executors+"-and-"+workerSlots);
//
//                        }


//                    else {
//                        cluster.assign(workerSlots.get(0), t.getId(), executors);
//                    }

                } catch (ParseException e) {
                e.printStackTrace();
            }

            for (String name : spouts.keySet()) {
                    String site1 = null;
                    SpoutSpec spout = spouts.get(name);
                JSONObject conf = null;
                try {
                    conf = (JSONObject) parser.parse(spout.get_common().get_json_conf());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                if (conf.get(SITE) != null && supervisors.get(conf.get(SITE)) != null) {
                        site1 = (String) conf.get(SITE);
                        System.out.println("TEST:inside topology loop for spout-" + site1);
                    }
                    SupervisorDetails supervisor = supervisors.get(site1);
                    List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
                    List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);
                    if (!workerSlots.isEmpty() && executors != null) {

                        cluster.assign(workerSlots.get(0), t.getId(), executors);
                    }
                }
            }

        }
    }


//        TEST:running
//        TEST:caching supervisors indexed by their sites in a hash map...
//        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@375112e4
//        TEST: checking if metadata is set on this supervisor....
//        TEST:Value for this supervisor-ufl
//        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@2baf531b
//        TEST: checking if metadata is set on this supervisor....
//        TEST:Value for this supervisor-tamu
//        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@3792805
//        TEST: checking if metadata is set on this supervisor....
//        TEST:Value for this supervisor-uh
//        ExecutorToComponents  mapping-
//        Component name--Second
//        start task-3
//        start task-3
//        Component name--__acker
//        start task-5
//        start task-5
//        Component name--Third
//        start task-4
//        start task-4
//        Component name--First
//        start task-2
//        start task-2
//        Component name--AuthSpout
//        start task-1
//        start task-1
//        TEST:inside topology loop for bolt-ufl
//        TEST:inside topology loop for bolt-tamu
//        TEST:inside topology loop for bolt-uh
//        TEST:inside topology loop for spout-tamu