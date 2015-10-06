package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class HsAllTasksBlock extends HtmlBlock{
	final AppContext appContext;
	final SimpleDateFormat dateFormat =
		    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");

	  @Inject HsAllTasksBlock(AppContext appctx) {
	    appContext = appctx;
	  }
	  
	@Override
	protected void render(Block html) {
		// TODO Auto-generated method stub
		TBODY<TABLE<Hamlet>> tbody = html.
			      h2("Retired Tasks").
			      table("#jobs").
			        thead().
			          tr().
			            th("Start Time").
			            th("Finish Time").
			            th("Task ID").
			            th("Task Type").
			            th("Task State").
			            th("Running Time").
			            th("Gc Count").
			            th("Gc Time").
			            th("Cpu Useage")._()._().
			        tbody();
		
		Map<TaskId, Task> taskLists;
		Task t;
		StringBuilder jobsTableData = new StringBuilder("[\n");
	    for (Job j : appContext.getAllJobs().values()) {
	    	JobId jobId = j.getID();
	    	Job jb = appContext.getJob(jobId);
	      taskLists = jb.getTasks();
	      
	      for(Entry<TaskId, Task> entry: taskLists.entrySet()){
	    	  t = entry.getValue();
	    	  long gcCount = -1;
	    	  long gcTotalTime = -1;
	    	  double cpuUseage = -1;
	    	  
	    	  if(t.getReport().getCounters() != null && t.getReport().getCounters().getCounter(TaskCounter.GC_COUNTERS) != null){
	    		  gcCount = t.getReport().getCounters().getCounter(TaskCounter.GC_COUNTERS).getValue();
	    	  }
	    	  
	    	  if(t.getReport().getCounters() != null && t.getReport().getCounters().getCounter(TaskCounter.GC_TIME_MILLIS) != null){
	    		  gcTotalTime = t.getReport().getCounters().getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
	    	  }
	    	  
	    	  if(t.getReport().getCounters() != null && t.getReport().getCounters().getCounter(TaskCounter.CPU_USAGE_PERCENTS) != null){
	    		  cpuUseage = t.getReport().getCounters().getCounter(TaskCounter.CPU_USAGE_PERCENTS).getValue();
	    	  }
	    	  
	    	  jobsTableData.append("[\"")
		      .append(dateFormat.format(new Date(t.getReport().getStartTime()))).append("\",\"")
		      .append(dateFormat.format(new Date(t.getReport().getFinishTime()))).append("\",\"")
		      .append(t.getID()).append("\",\"")
		      .append(t.getType().toString()).append("\",\"")
		      .append(t.getReport().getTaskState().toString()).append("\",\"")
		      .append(String.valueOf(secToTime(((t.getReport().getFinishTime()-t.getReport().getStartTime())/1000)))).append("\",\"")
		      .append(String.valueOf(gcCount)).append("\",\"")
		      .append(gcTotalTime).append("\",\"")
		      .append(String.valueOf(cpuUseage)).append("\"],\n");
	      }
	    }

	    //Remove the last comma and close off the array of arrays
	    if(jobsTableData.charAt(jobsTableData.length() - 2) == ',') {
	      jobsTableData.delete(jobsTableData.length()-2, jobsTableData.length()-1);
	    }
	    jobsTableData.append("]");
	    html.script().$type("text/javascript").
	    _("var jobsTableData=" + jobsTableData)._();
	    tbody._().
	    tfoot().
	      tr().
	        th().input("search_init").$type(InputType.text).$name("start_time").$value("Start Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("finish_time").$value("Finish Time")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        th().input("search_init").$type(InputType.text).$name("task_id").$value("Task ID")._()._().
	        _().
	      _().
	    _();
	}
	
	public static String secToTime(long time) {
	      String timeStr = null;
	      long hour = 0;
	      long minute = 0;
	      long second = 0;
	      if (time <= 0)
	          return "00:00:00";
	      else {
	          minute = time / 60;
	          if (minute < 60) {
	              second = time % 60;
	              timeStr = "00:" + unitFormat(minute) + ":" + unitFormat(second);
	          } else {
	              hour = minute / 60;
	              if (hour > 99)
	                  return "99:59:59";
	              minute = minute % 60;
	              second = time - hour * 3600 - minute * 60;
	              timeStr = unitFormat(hour) + ":" + unitFormat(minute) + ":" + unitFormat(second);
	          }
	      }
	      return timeStr;
	  }

	  public static String unitFormat(long i) {
	      String retStr = null;
	      if (i >= 0 && i < 10)
	          retStr = "0" + Long.toString(i);
	      else
	          retStr = "" + i;
	      return retStr;
	  }

}
