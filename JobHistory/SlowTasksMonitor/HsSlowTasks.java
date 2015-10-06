package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.yarn.webapp.SubView;

public class HsSlowTasks extends HsView{

	@Override
	protected Class<? extends SubView> content() {
		return HsSlowTasksBlock.class;
	}
}
