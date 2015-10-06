package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML;

public class HsAllTasksPage extends HsView {

	@Override
	protected Class<? extends SubView> content() {
		return HsAllTasksBlock.class;
	}

	@Override
	protected void preHead(HTML<_> html) {
		super.preHead(html);
	}

	private String jobsTableInit() {
		return tableInit()
				.append(", 'aaData': jobsTableData")
				.append(", bDeferRender: true")
				.append(", bProcessing: true")
				.

				// Sort by id upon page load
				append(", aaSorting: [[2, 'desc']]")
				.append(", aoColumnDefs:[")
				.
				// Maps Total, Maps Completed, Reduces Total and Reduces
				// Completed
				append("{'sType':'numeric', 'bSearchable': false, 'aTargets': [ 7, 8, 9, 10 ] }")
				.append("]}").toString();
	}

	private String jobsPostTableInit() {
		return "var asInitVals = new Array();\n"
				+ "$('tfoot input').keyup( function () \n{"
				+ "  jobsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"
				+ "} );\n"
				+ "$('tfoot input').each( function (i) {\n"
				+ "  asInitVals[i] = this.value;\n"
				+ "} );\n"
				+ "$('tfoot input').focus( function () {\n"
				+ "  if ( this.className == 'search_init' )\n"
				+ "  {\n"
				+ "    this.className = '';\n"
				+ "    this.value = '';\n"
				+ "  }\n"
				+ "} );\n"
				+ "$('tfoot input').blur( function (i) {\n"
				+ "  if ( this.value == '' )\n"
				+ "  {\n"
				+ "    this.className = 'search_init';\n"
				+ "    this.value = asInitVals[$('tfoot input').index(this)];\n"
				+ "  }\n" + "} );\n";
	}
}
