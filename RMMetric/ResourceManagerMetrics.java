package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

@InterfaceAudience.Private
@Metrics(about="ResourceManager metrics", context="rpc")
public class ResourceManagerMetrics {
	final MetricsRegistry registry = new MetricsRegistry("Resourcemanager");
	String name;
	
	public static final String KILL_APP_REQUEST = "Kill Application Request";
    public static final String SUBMIT_APP_REQUEST = "Submit Application Request";
    public static final String MOVE_APP_REQUEST = "Move Application Request";
    public static final String FINISH_SUCCESS_APP = "Application Finished - Succeeded";
    public static final String FINISH_FAILED_APP = "Application Finished - Failed";
    public static final String FINISH_KILLED_APP = "Application Finished - Killed";
    public static final String REGISTER_AM = "Register App Master";
    public static final String AM_ALLOCATE = "App Master Heartbeats";
    public static final String UNREGISTER_AM = "Unregister App Master";
    public static final String ALLOC_CONTAINER = "AM Allocated Container";
    public static final String RELEASE_CONTAINER = "AM Released Container";
    
	@Metric MutableCounterLong cmAllocatedSuccessOps;
	@Metric MutableCounterLong cmReleasedSuccessOps;
	@Metric MutableCounterLong killAppRequestSuccessOps;
	@Metric MutableCounterLong submitAppRequestSuccessOps;
	@Metric MutableCounterLong moveAppRequestSuccessOps;
	@Metric MutableCounterLong registerAMSuccessOps;
	@Metric MutableCounterLong unRegisterAMSuccessOps;
	@Metric MutableCounterLong amAllocatedSuccessOps;
	@Metric MutableCounterLong finishSucceedAppSuccessOps;
	@Metric MutableCounterLong finishFailedAppSuccessOps;
	@Metric MutableCounterLong finishKilledAppSuccessOps;
	
	@Metric MutableCounterLong cmAllocatedFailedOps;
	@Metric MutableCounterLong cmReleasedFailedOps;
	@Metric MutableCounterLong killAppRequestFailedOps;
	@Metric MutableCounterLong submitAppRequestFailedOps;
	@Metric MutableCounterLong moveAppRequestFailedOps;
	@Metric MutableCounterLong registerAMFailedOps;
	@Metric MutableCounterLong unRegisterAMFailedOps;
	@Metric MutableCounterLong amAllocatedFailedOps;
	@Metric MutableCounterLong finishSucceedAppFailedOps;
	@Metric MutableCounterLong finishFailedAppFailedOps;
	@Metric MutableCounterLong finishKilledAppFailedOps;
	
    	
	public ResourceManagerMetrics(String name){
		this.name = name;
	}
	
	public static ResourceManagerMetrics create(String name) {
		ResourceManagerMetrics m = new ResourceManagerMetrics(name);
	    return DefaultMetricsSystem.instance().register(m.name, null, m);
	}
	
	public void incrSucceedOpr(String opr){
		String result = "SUCCESS";
		
		switch (opr){
		case KILL_APP_REQUEST:
			incrKillAppRequestOpr(opr, result);
			break;
			
		case SUBMIT_APP_REQUEST:
			incrSubmitAppRequestOpr(opr, result);
			break;
			
		case MOVE_APP_REQUEST:
			incrMoveAppRequestOpr(opr, result);
			break;
			
		case FINISH_SUCCESS_APP:
			incrFinishSucceedAppOpr(opr, result);
			break;
			
		case FINISH_FAILED_APP:
			incrFinishFailedAppOpr(opr, result);
			break;
			
		case FINISH_KILLED_APP:
			incrFinishKilledAppOpr(opr, result);
			break;
			
		case REGISTER_AM:
			incrRegisterAMOpr(opr, result);
			break;
			
		case AM_ALLOCATE:
			incrAMAllocatedOpr(opr, result);
			break;
			
		case UNREGISTER_AM:
			incrUnRegisterAMOpr(opr, result);
			break;
			
		case ALLOC_CONTAINER:
			incrContainerAllcatedOpr(opr, result);
			break;
			
		case RELEASE_CONTAINER:
			incrContainerReleasedOpr(opr, result);
			break;
			
		default: 
			break;
		}
			
	}
	
	public void incrFailedOpr(String opr){
		String result = "FAILED";
		
		switch (opr){
		case KILL_APP_REQUEST:
			incrKillAppRequestOpr(opr, result);
			break;
			
		case SUBMIT_APP_REQUEST:
			incrSubmitAppRequestOpr(opr, result);
			break;
			
		case MOVE_APP_REQUEST:
			incrMoveAppRequestOpr(opr, result);
			break;
			
		case FINISH_SUCCESS_APP:
			incrFinishSucceedAppOpr(opr, result);
			break;
			
		case FINISH_FAILED_APP:
			incrFinishFailedAppOpr(opr, result);
			break;
			
		case FINISH_KILLED_APP:
			incrFinishKilledAppOpr(opr, result);
			break;
			
		case REGISTER_AM:
			incrRegisterAMOpr(opr, result);
			break;
			
		case AM_ALLOCATE:
			incrAMAllocatedOpr(opr, result);
			break;
			
		case UNREGISTER_AM:
			incrUnRegisterAMOpr(opr, result);
			break;
			
		case ALLOC_CONTAINER:
			incrContainerAllcatedOpr(opr, result);
			break;
			
		case RELEASE_CONTAINER:
			incrContainerReleasedOpr(opr, result);
			break;
			
		default: 
			break;
		}
	}
	
	public void incrContainerAllcatedOpr(String opr, String result){
		if(opr.equals(ALLOC_CONTAINER)){
			if(result.equals("SUCCESS")){
				cmAllocatedSuccessOps.incr();
			}else if (result.equals("FAILED")){
				cmAllocatedFailedOps.incr();
			}
		}
	}
	
	public void incrContainerReleasedOpr(String opr, String result){
		if(opr.equals(RELEASE_CONTAINER)){
			if(result.equals("SUCCESS")){
				cmReleasedSuccessOps.incr();
			}else if (result.equals("FAILED")){
				cmReleasedFailedOps.incr();
			}
		}
	}
	
	public void incrKillAppRequestOpr(String opr, String result){
		if(opr.equals(KILL_APP_REQUEST)){
			if(result.equals("SUCCESS")){
				killAppRequestSuccessOps.incr();
			}else if (result.equals("FAILED")){
				killAppRequestFailedOps.incr();
			}
		}
	}
	
	public void incrSubmitAppRequestOpr(String opr, String result){
		if(opr.equals(SUBMIT_APP_REQUEST)){
			if(result.equals("SUCCESS")){
				submitAppRequestSuccessOps.incr();
			}else if (result.equals("FAILED")){
				submitAppRequestFailedOps.incr();
			}
		}
	}
	
	public void incrMoveAppRequestOpr(String opr, String result){
		if(opr.equals(MOVE_APP_REQUEST)){
			if(result.equals("SUCCESS")){
				moveAppRequestSuccessOps.incr();
			}else if (result.equals("FAILED")){
				moveAppRequestFailedOps.incr();
			}
		}
	}
	
	public void incrRegisterAMOpr(String opr, String result){
		if(opr.equals(REGISTER_AM)){
			if(result.equals("SUCCESS")){
				registerAMSuccessOps.incr();
			}else if (result.equals("FAILED")){
				registerAMFailedOps.incr();
			}
		}
	}
	
	public void incrUnRegisterAMOpr(String opr, String result){
		if(opr.equals(UNREGISTER_AM)){
			if(result.equals("SUCCESS")){
				unRegisterAMSuccessOps.incr();
			}else if (result.equals("FAILED")){
				unRegisterAMFailedOps.incr();
			}
		}
	}
	
	public void incrAMAllocatedOpr(String opr, String result){
		if(opr.equals(AM_ALLOCATE)){
			if(result.equals("SUCCESS")){
				amAllocatedSuccessOps.incr();
			}else if (result.equals("FAILED")){
				amAllocatedFailedOps.incr();
			}
		}
	}
	
	public void incrFinishSucceedAppOpr(String opr, String result){
		if(opr.equals(FINISH_SUCCESS_APP)){
			if(result.equals("SUCCESS")){
				finishSucceedAppSuccessOps.incr();
			}else if (result.equals("FAILED")){
				finishSucceedAppFailedOps.incr();
			}
		}
	}
	
	public void incrFinishFailedAppOpr(String opr, String result){
		if(opr.equals(FINISH_FAILED_APP)){
			if(result.equals("SUCCESS")){
				finishFailedAppSuccessOps.incr();
			}else if (result.equals("FAILED")){
				finishFailedAppFailedOps.incr();
			}
		}
	}
	
	public void incrFinishKilledAppOpr(String opr, String result){
		if(opr.equals(FINISH_KILLED_APP)){
			if(result.equals("SUCCESS")){
				finishKilledAppSuccessOps.incr();
			}else if (result.equals("FAILED")){
				finishKilledAppFailedOps.incr();
			}
		}
	}
}
