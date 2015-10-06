package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.SLOW_TASKS_TYPE;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobInfo;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.InputType;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

public class HsSlowTasksBlock extends HtmlBlock{
	final AppContext appContext;
	final SimpleDateFormat dateFormat =
		    new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");

	  @Inject HsSlowTasksBlock(AppContext appctx) {
	    appContext = appctx;
	  }
	
	 /* @Override
		protected void render(Block html) {
		  TBODY<TABLE<Hamlet>> tbody = html.
			      h2("Retired Jobs").
			      table("#jobs").
			        thead().
			          tr().
			            th("Submit Time").
			            th("Start Time").
			            th("Finish Time").
			            th(".id", "Job ID").
			            th(".name", "Name").
			            th("User").
			            th("Queue").
			            th(".state", "State").
			            th("Maps Total").
			            th("Maps Completed").
			            th("Reduces Total").
			            th("Reduces Completed").
			            th("Running Time").
			            th("UberTask").
			            th("Avg MapTime").
			            th("Avg MergeTime").
			            th("Avg ShuffleTime").
			            th("Avg ReduceTime").
			            th("Failed MapAttempts").
			            th("Failed ReduceAttempts").
			            th("Successed MapAttempts").
			            th("Successed ReduceAttempts")._()._().
			        tbody();
			    LOG.info("Getting list of all Jobs.");
			    // Write all the data into a JavaScript array of arrays for JQuery
			    // DataTables to display
			    StringBuilder jobsTableData = new StringBuilder("[\n");
			    for (Job j : appContext.getAllJobs().values()) {
			    	JobId jobId = j.getID();
			    	Job jb = appContext.getJob(jobId);
			      JobInfo job = new JobInfo(jb);
			      
			      jobsTableData.append("[\"")
			      .append(dateFormat.format(new Date(job.getSubmitTime()))).append("\",\"")
			      .append(dateFormat.format(new Date(job.getStartTime()))).append("\",\"")
			      .append(dateFormat.format(new Date(job.getFinishTime()))).append("\",\"")
			      .append("<a href='").append(url("job", job.getId())).append("'>")
			      .append(job.getId()).append("</a>\",\"")
			      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
			        job.getName()))).append("\",\"")
			      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
			        job.getUserName()))).append("\",\"")
			      .append(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeHtml(
			        job.getQueueName()))).append("\",\"")
			      .append(job.getState()).append("\",\"")
			      .append(String.valueOf(job.getMapsTotal())).append("\",\"")
			      .append(String.valueOf(job.getMapsCompleted())).append("\",\"")
			      .append(String.valueOf(job.getReducesTotal())).append("\",\"")
			      .append(String.valueOf(job.getReducesCompleted())).append("\",\"")
			      .append(String.valueOf(secToTime(((job.getFinishTime()-job.getStartTime())/1000)))).append("\",\"")
			      .append(job.isUber()).append("\",\"")
			      .append(String.valueOf(secToTime(job.getAvgMapTime()/1000))).append("\",\"")
			      .append(String.valueOf(secToTime(job.getAvgMergeTime()/1000))).append("\",\"")
			      .append(String.valueOf(secToTime(job.getAvgShuffleTime()/1000))).append("\",\"")
			      .append(String.valueOf(secToTime(job.getAvgReduceTime()/1000))).append("\",\"")
			      .append(String.valueOf(job.getFailedMapAttempts())).append("\",\"")
			      .append(String.valueOf(job.getFailedReduceAttempts())).append("\",\"")
			      .append(String.valueOf(job.getSuccessfulMapAttempts())).append("\",\"")
			      .append(String.valueOf(job.getSuccessfulReduceAttempts())).append("\"],\n");
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
			        th().input("search_init").$type(InputType.text).$name("submit_time").$value("Submit Time")._()._().
			        th().input("search_init").$type(InputType.text).$name("start_time").$value("Start Time")._()._().
			        th().input("search_init").$type(InputType.text).$name("finish_time").$value("Finish Time")._()._().
			        th().input("search_init").$type(InputType.text).$name("job_id").$value("Job ID")._()._().
			        th().input("search_init").$type(InputType.text).$name("name").$value("Name")._()._().
			        th().input("search_init").$type(InputType.text).$name("user").$value("User")._()._().
			        th().input("search_init").$type(InputType.text).$name("queue").$value("Queue")._()._().
			        th().input("search_init").$type(InputType.text).$name("state").$value("State")._()._().
			        th().input("search_init").$type(InputType.text).$name("maps_total").$value("Maps Total")._()._().
			        th().input("search_init").$type(InputType.text).$name("maps_completed").$value("Maps Completed")._()._().
			        th().input("search_init").$type(InputType.text).$name("reduces_total").$value("Reduces Total")._()._().
			        th().input("search_init").$type(InputType.text).$name("reduces_completed").$value("Reduces Completed")._()._().
			        th().input("search_init").$type(InputType.text).$name("running_time").$value("RunningTime")._()._().
			        th().input("search_init").$type(InputType.text).$name("uber_task").$value("UberTask")._()._().
			        th().input("search_init").$type(InputType.text).$name("avg_maptime").$value("Avg MapTime")._()._().
			        th().input("search_init").$type(InputType.text).$name("avg_mergeTime").$value("Avg MergeTime")._()._().
			        th().input("search_init").$type(InputType.text).$name("avg_shuffleTime").$value("Avg ShuffleTime")._()._().
			        th().input("search_init").$type(InputType.text).$name("avg_reduceTime").$value("Avg ReduceTime")._()._().
			        th().input("search_init").$type(InputType.text).$name("failed_mapAttempts").$value("Failed MapAttempts")._()._().
			        th().input("search_init").$type(InputType.text).$name("failed_reduceAttempts").$value("Failed ReduceAttempts")._()._().
			        th().input("search_init").$type(InputType.text).$name("successed_mapAttempts").$value("Successed MapAttempts")._()._().
			        th().input("search_init").$type(InputType.text).$name("successed_reduceAttempts").$value("Successed ReduceAttempts")._()._().
			        _().
			      _().
			    _();
	  } */
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
		
		Map<TaskId, Task> taskMap;
		Task[] tasks;
		Task t;
		TaskReport report;
		double avgRunningTime;
		double avgGcTime;
		
		StringBuilder jobsTableData = new StringBuilder("[\n");
	    for (Job j : appContext.getAllJobs().values()) {
	    	JobId jobId = j.getID();
	    	Job jb = appContext.getJob(jobId);
	    	JobInfo job = new JobInfo(jb);
	    	taskMap = jb.getTasks();
	        tasks = taskMap.values().toArray(new Task[taskMap.size()]);
	        
	        avgRunningTime = 0;
	        avgGcTime = 0;
	        for(Task t2: tasks){
	        	avgRunningTime += t2.getReport().getFinishTime() - t2.getReport().getStartTime();
	        	avgGcTime += t2.getReport().getCounters().getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
	        }
	        avgRunningTime /= (job.getMapsCompleted() + job.getReducesCompleted());
	        avgGcTime /= (job.getMapsCompleted() + job.getReducesCompleted());
	      for(Entry<TaskId, Task> entry: taskMap.entrySet()){
	    	  t = entry.getValue();
	    	  long gcCount = -1;
	    	  long gcTotalTime = -1;
	    	  double cpuUseage = -1;
	    	  double tmpRunningTime;
	    	  double tmpGcTime;
	    	  
	    	  report = t.getReport();
	    	  tmpRunningTime = report.getFinishTime() - report.getStartTime();
	    	  tmpGcTime = report.getCounters().getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
	    	  
	    	  if(tmpRunningTime < avgRunningTime && $(SLOW_TASKS_TYPE).equals("runningtime")){
	    		  continue;
	    	  }
	    	  
	    	  if(tmpGcTime < avgGcTime && $(SLOW_TASKS_TYPE).equals("gctime")){
	    		  continue;
	    	  }
	    	  
	    	  if(report.getCounters() != null && report.getCounters().getCounter(TaskCounter.GC_COUNTERS) != null){
	    		  gcCount = report.getCounters().getCounter(TaskCounter.GC_COUNTERS).getValue();
	    	  }
	    	  
	    	  if(report.getCounters() != null && report.getCounters().getCounter(TaskCounter.GC_TIME_MILLIS) != null){
	    		  gcTotalTime = report.getCounters().getCounter(TaskCounter.GC_TIME_MILLIS).getValue();
	    	  }
	    	  
	    	  if(report.getCounters() != null && report.getCounters().getCounter(TaskCounter.CPU_USAGE_PERCENTS) != null){
	    		  cpuUseage = report.getCounters().getCounter(TaskCounter.CPU_USAGE_PERCENTS).getValue();
	    	  }
	    	  
	    	  jobsTableData.append("[\"")
		   //   .append(dateFormat.format(new Date(report.getStartTime()))).append("\",\"")
		   //   .append(dateFormat.format(new Date(report.getFinishTime()))).append("\",\"")
		      .append(t.getID()).append("\",\"")
		   //   .append(t.getType().toString()).append("\",\"")
		   //   .append(report.getTaskState().toString()).append("\",\"")
		   //   .append(String.valueOf(secToTime(((report.getFinishTime()-report.getStartTime())/1000)))).append("\",\"")
		   //   .append(String.valueOf(gcCount)).append("\",\"")
		   //   .append(gcTotalTime).append("\",\"")
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
