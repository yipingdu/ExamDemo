package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {
    //用于存放服务节点
    private List<Integer> nodeList = new ArrayList<Integer>();
    //用于存放任务节点
    private Map<Integer,Integer> taskMap =  new HashMap<Integer,Integer>();
    //用于存放挂起的任务
    private List<Integer> taskQueue = new LinkedList<Integer>();
    //用于存放运行中的服务节点和任务的对应关系
    private Map<Integer,Map<Integer,Integer>> taskInfoMap = new HashMap<Integer,Map<Integer,Integer>>();
    //保存所有任务状态列表,用于查询
    private List<TaskInfo> tasks = new ArrayList<TaskInfo>();


    public int init() {
        nodeList.clear();
        taskMap.clear();
        taskQueue.clear();
        taskInfoMap.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        //合法性校验
        if(nodeId <= 0){
            return ReturnCodeKeys.E004;
        }
        //服务存在
        if(nodeList.contains(nodeId)) {
            return ReturnCodeKeys.E005;
        }
        nodeList.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        //非法性校验
        if(nodeId <= 0 ){
            return ReturnCodeKeys.E004;
        }
        //服务节点不存在
        if(!nodeList.contains(nodeId)){
            return ReturnCodeKeys.E007;
        }
        //获取当前服务节点下的任务
        Map<Integer,Integer> nodeTask = taskInfoMap.get(nodeId);
        if(null != nodeTask && !nodeTask.isEmpty()){
            for( int key : nodeTask.keySet()){
                taskQueue.add(key);
            }
        }
        boolean deletaFlag =  nodeList.remove(Integer.valueOf(nodeId));
        if(deletaFlag){
            return ReturnCodeKeys.E006;
        }
        else{
            return ReturnCodeKeys.E007;
        }
    }


    public int addTask(int taskId, int consumption) {
        //非法性校验
        if(taskId <= 0 ){
            return ReturnCodeKeys.E009;
        }
        //任务已添加
        if(taskMap.containsKey(taskId)){
            return ReturnCodeKeys.E010;
        };
        taskMap.put(taskId,consumption);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        //非法性校验
        if(taskId <= 0 ){
            return ReturnCodeKeys.E009;
        }
        //任务不存在
        if(!taskMap.containsKey(taskId)){
            return ReturnCodeKeys.E012;
        }
        taskMap.remove(taskId);
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        //非法性校验
        if(threshold <= 0 ){
            return ReturnCodeKeys.E002;
        }
        //如果挂起队列有任务存在
        //总消耗率
        Map<Integer ,Integer > totalConsumption = new HashMap<Integer ,Integer>();
        //任务总数
        Map<Integer ,Integer > totalTask = new HashMap<Integer ,Integer>();
        for(int nodeId :taskInfoMap.keySet()){
            int cnsumption = 0;
            int task = 0;
            Map<Integer,Integer> runningTask =  taskInfoMap.get(nodeId);
            for(int taskId : runningTask.keySet()){
                cnsumption = cnsumption + runningTask.get(taskId);
                task++;
            }
            totalConsumption.put(nodeId,cnsumption);
            totalTask.put(nodeId,task);
            for(int taskId : taskQueue){
                //当前消耗率
                int con = taskMap.get(taskId);
//                for(){
//
//                }
            }
        }

        //没有挂起任务
        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        //清空tasks
        tasks.clear();
        //查询挂起
        for(int taskId : taskQueue){
            TaskInfo task = new TaskInfo();
            task.setNodeId(-1);
            task.setTaskId(taskId);
        }
        //查询在运行任务
        for( int key : taskInfoMap.keySet()){
            Map<Integer,Integer> runningTask = taskInfoMap.get(key);
            for( int taskKey : runningTask.keySet()){
                TaskInfo task = new TaskInfo();
                task.setNodeId(key);
                task.setTaskId(taskKey);
                tasks.add(task);
            }
        }
        //参数列表非法
        if(null == tasks){
            return ReturnCodeKeys.E016;
        }
        return ReturnCodeKeys.E015;
    }

}
