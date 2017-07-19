package main.java;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SiteAwareScheduler implements IScheduler {

    private static final String SITE = "site";

    @Override
    public void prepare(Map conf) {}

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("TEST:running");
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
        Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
        JSONParser parser = new JSONParser();

        System.out.println("TEST:caching supervisors indexed by their sites in a hash map...");
        for (SupervisorDetails s : supervisorDetails) {
            System.out.println("TEST:supervisor name-"+s);
            Map<String, String> metadata = (Map<String, String>) s.getSchedulerMeta();
            if (metadata.get(SITE) != null) {
                System.out.println("TEST: checking if metadata is set on this supervisor....");
                supervisors.put((String) metadata.get(SITE), s);
                System.out.println("TEST:Value for this supervisor-" + (String) metadata.get(SITE));
            }
        }

        for (TopologyDetails t : topologyDetails) {
            //testing
            Map<ExecutorDetails,String> EtoCmapping=cluster.getNeedsSchedulingExecutorToComponents(t);
            System.out.println("ExecutorToComponents  mapping-");
            for(ExecutorDetails e:EtoCmapping.keySet())
            {
                System.out.println("Component name--"+EtoCmapping.get(e));
                System.out.println("start task-"+e.getStartTask());
                System.out.println("end task-"+e.getEndTask());
            }
            //testing

            if (!cluster.needsScheduling(t)) continue;
            StormTopology topology = t.getTopology();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            try {
                String site = null;
                for (String name : bolts.keySet()) {
                    Bolt bolt = bolts.get(name);
                    JSONObject conf = (JSONObject) parser.parse(bolt.get_common().get_json_conf());
                    if (conf.get(SITE) != null && supervisors.get(conf.get(SITE)) != null) {  //verify the site nmae on
                        site = (String) conf.get(SITE);
                        System.out.println("TEST:inside topology loop for bolt-" + site);
                    }

                    SupervisorDetails supervisor = supervisors.get(site);
                    List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
                    List<ExecutorDetails> executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(name);  //getting list of executors for a  bolt

                    System.out.println("executor list for that bolt- "+executors);

                    if (!workerSlots.isEmpty() && executors != null) {
                        cluster.assign(workerSlots.get(0), t.getId(), executors);
                    }
                }

                for (String name : spouts.keySet()) {
                    String site1 = null;
                    SpoutSpec spout = spouts.get(name);
                    JSONObject conf = (JSONObject) parser.parse(spout.get_common().get_json_conf());
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
            } catch (ParseException e) {
                e.printStackTrace();
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