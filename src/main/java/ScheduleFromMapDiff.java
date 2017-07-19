package main.java;

//import backtype.storm.scheduler.*;
import org.apache.commons.collections.MapUtils;
import org.apache.storm.scheduler.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by anshushukla on 17/03/16.
 */
public class ScheduleFromMapDiff {


    public static List<ExecutorDetails> getExecListFromBoltname(TopologyDetails t, Cluster cluster, int no_of_exec, String bolt_name_from_conf) {
        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
        executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(bolt_name_from_conf);
        System.out.println("executors within this bolt- " + bolt_name_from_conf + "-" + executors);
        List<ExecutorDetails> executorDetailses = null;

        if (executors != null) {
            if (executors.size() >= no_of_exec) {
                executorDetailses = new ArrayList(executors.subList(0, no_of_exec));
            } else {
                System.out.println("\n\n\t\t\t\tsize mismatch Check conf******-" + "executorsSize-" + executors.size() + "-no_of_exec-" + no_of_exec);
            }
        }

        System.out.println("executorDetailses after slicing-" + executorDetailses);

        return executorDetailses;
    }


    public static WorkerSlot getSlotFromSupIDandPort(String slot_conf_key, Map<String, String> vm_supIDMap_Name, Map<String, SupervisorDetails> supervisors, Cluster cluster) {
        String supID_from_slot_conf_key = slot_conf_key.split(":")[0];
        String vm_name = vm_supIDMap_Name.get(supID_from_slot_conf_key);
        int port_no_from_conf = Integer.parseInt(slot_conf_key.split(":")[1]);
        SupervisorDetails supervisor = supervisors.get(vm_name);
        List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);

        if (!workerSlots.isEmpty()) {//finding the correct workerslot by port number
            for (WorkerSlot w : workerSlots) {
                if (w.getPort() == port_no_from_conf) {
                    return w;
                }
            }
        } else {
            System.out.println("\n\nEither _ws is empty or Mismatch from conf CHECK port number \n\n -" + port_no_from_conf);
        }
        return null;
    }

    public static void freeSlotIfAnyUnscheduledBolt(String slot_conf_key, HashMap<String, Integer> slot_conf_val, HashMap<WorkerSlot, HashMap<String, Integer>> currentexecToboltNameMap, TopologyDetails t, Cluster cluster, WorkerSlot _ws) {
        //look for all bolts of that slot
        for (String bolt_name_from_conf : slot_conf_val.keySet()) {
            int no_of_exec_conf = slot_conf_val.get(bolt_name_from_conf);
            if (currentexecToboltNameMap.get(slot_conf_key).containsKey(bolt_name_from_conf)) {
                int no_of_exec_curr = currentexecToboltNameMap.get(slot_conf_key).get(bolt_name_from_conf);
                int exec_rem_for_sche = no_of_exec_conf - no_of_exec_curr;
                if (exec_rem_for_sche != 0) {
                    cluster.freeSlot(_ws);
                    System.out.println("Slot is being released-" + _ws);
                    break;
                } else {
                    System.out.println("All exec of bolt are scheduled" + bolt_name_from_conf + "-" + slot_conf_key);
                }
            } else if (!currentexecToboltNameMap.get(slot_conf_key).containsKey(bolt_name_from_conf)) {//if any bolt is not scheduled till now over that slot
                cluster.freeSlot(_ws);
            }
        }
    }

//        {97402cb2-41ce-4d85-ab9d-c616dbec7eae:6710={Second=1}, 97402cb2-41ce-4d85-ab9d-c616dbec7eae:6709={Third=1}, 97402cb2-41ce-4d85-ab9d-c616dbec7eae:6713={Second=2},
// 4ce82bb9-e8c2-457d-b408-1cadf0ec29e3:6719={Third=1, Second=1}, 9a897b42-1893-42bf-91d2-918e2f374df8:6702={First=1}, 4ce82bb9-e8c2-457d-b408-1cadf0ec29e3:6718={Second=6, First=1}, 4ce82bb9-e8c2-457d-b408-1cadf0ec29e3:6717={First=6}, 9a897b42-1893-42bf-91d2-918e2f374df8:6701={First=2}}

    public static void findMatrixDiffandSchedule(HashMap<WorkerSlot, HashMap<String, Integer>> currentexecToboltNameMap, HashMap<String, HashMap<String, Integer>> execToboltNameMap_from_Conf, Map<String, String> vm_Name_supIDMap, Map<String, SupervisorDetails> supervisors, Cluster cluster, TopologyDetails t) {
        System.out.println("Inside findMatrixDiff function !!");
        Map<String, String> vm_supIDMap_Name = MapUtils.invertMap(vm_Name_supIDMap);
        System.out.println("\nexecToboltNameMap_from_Conf\n" + execToboltNameMap_from_Conf);
        System.out.println("\ncurrentexecToboltNameMap\n" + currentexecToboltNameMap);


        for (String slot_conf_key : execToboltNameMap_from_Conf.keySet()) {
            HashMap<String, Integer> slot_conf_val = execToboltNameMap_from_Conf.get(slot_conf_key);
            System.out.println("\t\tScheduling for slot-" + slot_conf_val + "*****");
            List<ExecutorDetails> _executorDetailses = new ArrayList();
            WorkerSlot _ws = null;
            {//TODO:contains key is comparing string with workslot type ref
                System.out.println("log3-" + new ArrayList<>(currentexecToboltNameMap.keySet()));
                System.out.println("log3-" + slot_conf_key + new ArrayList<>(currentexecToboltNameMap.keySet()).toString().contains(slot_conf_key));
                if (!currentexecToboltNameMap.containsKey(slot_conf_key)) {//if that slot is never used get executor list for that

                    for (String bolt_name_from_conf : slot_conf_val.keySet()) {
                        int no_of_exec_conf = slot_conf_val.get(bolt_name_from_conf);
                        if (no_of_exec_conf != 0) {
                            List<ExecutorDetails> temp_executorDetailses = ScheduleFromMapDiff.getExecListFromBoltname(t, cluster, no_of_exec_conf, bolt_name_from_conf);
                            if (temp_executorDetailses != null)
                                _executorDetailses.addAll(temp_executorDetailses);
                            System.out.println("LOG1-" + _executorDetailses);
                        }
                    }


                    _ws = ScheduleFromMapDiff.getSlotFromSupIDandPort(slot_conf_key, vm_supIDMap_Name, supervisors, cluster);
                    if (_executorDetailses.size() != 0 && _executorDetailses != null && _ws != null) {
                        cluster.assign(_ws, t.getId(), _executorDetailses);
                    }

                } else {//if slot entry is there in current state
                    _ws = ScheduleFromMapDiff.getSlotFromSupIDandPort(slot_conf_key, vm_supIDMap_Name, supervisors, cluster);
                    System.out.println("_ws inside else-" + _ws);
                    if (_ws != null) {
                        ScheduleFromMapDiff.freeSlotIfAnyUnscheduledBolt(slot_conf_key, slot_conf_val, currentexecToboltNameMap, t, cluster, _ws);
                        for (String bolt_name_from_conf : slot_conf_val.keySet()) {
                            int no_of_exec_conf = slot_conf_val.get(bolt_name_from_conf);
                            if (no_of_exec_conf != 0) {
                                List<ExecutorDetails> temp_executorDetailses = ScheduleFromMapDiff.getExecListFromBoltname(t, cluster, no_of_exec_conf, bolt_name_from_conf);
                                if (temp_executorDetailses != null)
                                    _executorDetailses.addAll(temp_executorDetailses);
                                System.out.println("LOG2-" + _executorDetailses);
                            }
                        }
                        if (_executorDetailses.size() != 0 && _executorDetailses != null)
                            cluster.assign(_ws, t.getId(), _executorDetailses);
                    }
                }
            }
        }
    }
}

