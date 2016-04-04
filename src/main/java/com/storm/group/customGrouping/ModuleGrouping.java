package com.storm.group.customGrouping;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ModuleGrouping implements CustomStreamGrouping, Serializable {
    private WorkerTopologyContext _ctx;  
    private List<Integer> _targetTasks;  
      
    public ModuleGrouping(){  
          
    }  
      @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    	// TODO Auto-generated method stub
          _ctx = context;  
          _targetTasks = targetTasks;  
    }

      @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
  		System.out.println("00000000000000000000：" + _targetTasks);
  		// 在这里我们采用单词首字母字符的整数值与任务数的余数，决定接收元组的bolt。
  		List<Integer> boltIds = null;
  		int index = 0;
          if(values.size()>0){
              String str = values.get(0).toString();
              if(!str.isEmpty()){
              	index = str.charAt(0) % _targetTasks.size();
              }
          }
          boltIds = Arrays.asList(_targetTasks.get(index));  
  		System.out.println("00000000000000000000111111111：" + boltIds);
          return boltIds;
  	}
}
