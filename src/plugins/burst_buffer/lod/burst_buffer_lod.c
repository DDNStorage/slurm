/*****************************************************************************\
 *  burst_buffer_generic.c - Generic library for managing a burst_buffer
 *****************************************************************************
 *  Copyright (C) 2014-2015 SchedMD LLC.
 *  Written by Morris Jette <jette@schedmd.com>
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <poll.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>

#include "slurm/slurm.h"

#include "src/common/list.h"
#include "src/common/pack.h"
#include "src/common/parse_config.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/timers.h"
#include "src/common/uid.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/run_command.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/plugins/burst_buffer/common/burst_buffer_common.h"

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *      <application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "burst_buffer" for Slurm burst_buffer) and <method> is a
 * description of how this plugin satisfies that application.  Slurm will only
 * load a burst_buffer plugin if the plugin_type string has a prefix of
 * "burst_buffer/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[]        = "burst_buffer lustre_on_demand plugin";
const char plugin_type[]        = "burst_buffer/lod";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

bool lustre_on_demand = false;
bool lod_started = false;
bool lod_setup = false;
bool lod_stage_out = false;
bool lod_stage_in = false;

/* LOD options */
char *nodes = NULL;
char *mdtdevs = NULL;
char *ostdevs = NULL;
char *inet = NULL;
char *mountpoint = NULL;

/* stage_in */
char *sin_src  = NULL;
char *sin_dest = NULL;

/* stage_out */
char *sout_src  = NULL;
char *sout_dest = NULL;

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
extern int init(void)
{
	debug2("RDEBUG : in init");
	return SLURM_SUCCESS;
}

/*
 * fini() is called when the plugin is unloaded. Free all memory.
 */
extern int fini(void)
{
	debug2("RDEBUG : in fini");
	return SLURM_SUCCESS;
}

/*
 * Return the total burst buffer size in MB
 */
extern uint64_t bb_p_get_system_size(void)
{
	uint64_t size = 0;
	return size;
}

/*
 * Load the current burst buffer state (e.g. how much space is available now).
 * Run at the beginning of each scheduling cycle in order to recognize external
 * changes to the burst buffer state (e.g. capacity is added, removed, fails,
 * etc.)
 *
 * init_config IN - true if called as part of slurmctld initialization
 * Returns a Slurm errno.
 */
extern int bb_p_load_state(bool init_config)
{
	debug2("RDEBUG : in bb_p_load_state");
	return SLURM_SUCCESS;
}

/*
 * Return string containing current burst buffer status
 * argc IN - count of status command arguments
 * argv IN - status command arguments
 * RET status string, release memory using xfree()
 */
extern char *bb_p_get_status(uint32_t argc, char **argv)
{
	debug2("RDEBUG : in bb_p_get_status");
	return NULL;
}

/*
 * Note configuration may have changed. Handle changes in BurstBufferParameters.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_reconfig(void)
{
	debug2("RDEBUG : in bb_p_reconfig");
	return SLURM_SUCCESS;
}

/*
 * Pack current burst buffer state information for network transmission to
 * user (e.g. "scontrol show burst")
 *
 * Returns a Slurm errno.
 */
extern int bb_p_state_pack(uid_t uid, Buf buffer, uint16_t protocol_version)
{
	debug2("RDEBUG : in bb_p_state_pack");
	return SLURM_SUCCESS;
}

/* Perform basic burst_buffer option validation */
static int _parse_bb_opts(struct job_descriptor *job_desc, uint64_t *bb_size,
                         uid_t submit_uid)
{
	char *bb_script, *save_ptr = NULL;
	char *tok;
	char *sub_tok;
	int rc = SLURM_SUCCESS;

	if (job_desc->script != NULL) {
		bb_script = xstrdup(job_desc->script);
		tok = strtok_r(bb_script, "\n", &save_ptr);
		while (tok) {
			if (tok[0] != '#')
				break;  /* Quit at first non-comment */

			if ((tok[1] == 'L') && (tok[2] == 'O') && (tok[3] == 'D')) {
				lustre_on_demand = true;
				tok+=4;
				debug2("RDEBUG: _parse_bb_opts found Lustre On Demand");
			}

			if (lustre_on_demand) {
				while (isspace(tok[0]))
					tok++;
				/* setup */
				if (!strncmp(tok, "setup", 5)) {
					lod_setup = true;
					if ((sub_tok = strstr(tok, "node="))) {
						nodes = xstrdup(sub_tok + 5);
						if ((sub_tok = strchr(nodes, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found node=%s", nodes);
					}

					if ((sub_tok = strstr(tok, "mdtdevs="))) {
						mdtdevs = xstrdup(sub_tok + 8);
						if ((sub_tok = strchr(mdtdevs, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found mdtdevs=%s", mdtdevs);
					}

					if ((sub_tok = strstr(tok, "ostdevs="))) {
						ostdevs = xstrdup(sub_tok + 8);
						if ((sub_tok = strchr(ostdevs, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found ostdevs=%s", ostdevs);
					}

					if ((sub_tok = strstr(tok, "inet="))) {
						inet = xstrdup(sub_tok + 5);
						if ((sub_tok = strchr(inet, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found inet=%s", inet);
					}

					if ((sub_tok = strstr(tok, "mountpoint="))) {
						mountpoint = xstrdup(sub_tok + 11);
						if ((sub_tok = strchr(mountpoint, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found mountpoint=%s", mountpoint);
					}
				} else if (!strncmp(tok, "stage_in", 8)) {
					lod_stage_in = true;
					debug2("RDEBUG: _parse_bb_opts in stage_in");
					if ((sub_tok = strstr(tok, "source="))) {
						sin_src = xstrdup(sub_tok + 7);
						if ((sub_tok = strchr(sin_src, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found sin_src=%s",
						       sin_src);
					}
					if ((sub_tok = strstr(tok, "destination="))) {
						sin_dest = xstrdup(sub_tok + 12);
						if ((sub_tok = strchr(sin_dest, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found sin_dest=%s",
						       sin_dest);
					}
				} else if (!strncmp(tok, "stage_out", 8)) {
					lod_stage_out = true;
					debug2("RDEBUG: _parse_bb_opts in stage_out");
					if ((sub_tok = strstr(tok, "source="))) {
						sout_src = xstrdup(sub_tok + 7);
						if ((sub_tok = strchr(sout_src, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found sout_src=%s",
						       sout_src);
					}
					if ((sub_tok = strstr(tok, "destination="))) {
						sout_dest = xstrdup(sub_tok + 12);
						if ((sub_tok = strchr(sout_dest, ' ')))
							sub_tok[0] = '\0';
						debug2("RDEBUG: _parse_bb_opts found sout_dest=%s",
						       sout_dest);
					}
				}
			}

			tok = strtok_r(NULL, "\n", &save_ptr);
		}
		xfree(bb_script);
	} else {
		debug2("RDEBUG: _parse_bb_opts no tok to parse");
	}

	debug2("RDEBUG: _parse_bb_opts return");
	return rc;
}

/*
 * Preliminary validation of a job submit request with respect to burst buffer
 * options. Performed after setting default account + qos, but prior to
 * establishing job ID or creating script file.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_validate(struct job_descriptor *job_desc,
			     uid_t submit_uid)
{
	uint64_t bb_size = 0;
	int rc = SLURM_SUCCESS;

	xassert(job_desc);
	xassert(job_desc->tres_req_cnt);

	debug2("RDEBUG : in bb_p_job_validate before parsing");

	rc = _parse_bb_opts(job_desc, &bb_size, submit_uid);

	debug2("RDEBUG : in bb_p_job_validate after parsing");
	return rc;
}

/*
 * Secondary validation of a job submit request with respect to burst buffer
 * options. Performed after establishing job ID and creating script file.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_validate2(struct job_record *job_ptr, char **err_msg)
{
	debug2("RDEBUG : in bb_p_job_validate2");
	return SLURM_SUCCESS;
}

/*
 * Fill in the tres_cnt (in MB) based off the job record
 * NOTE: Based upon job-specific burst buffers, excludes persistent buffers
 * IN job_ptr - job record
 * IN/OUT tres_cnt - fill in this already allocated array with tres_cnts
 * IN locked - if the assoc_mgr tres read locked is locked or not
 */
extern void bb_p_job_set_tres_cnt(struct job_record *job_ptr,
				  uint64_t *tres_cnt,
				  bool locked)
{
	debug2("RDEBUG : in bb_p_job_set_tres_cnt");
}

/*
 * For a given job, return our best guess if when it might be able to start
 */
extern time_t bb_p_job_get_est_start(struct job_record *job_ptr)
{
	time_t est_start = time(NULL);
	return est_start;
}

/*
 * Attempt to allocate resources and begin file staging for pending jobs.
 */
extern int bb_p_job_try_stage_in(List job_queue)
{
	debug2("RDEBUG : in bb_p_job_try_stage_in");
	return SLURM_SUCCESS;
}

/*
 * Determine if a job's burst buffer stage-in is complete
 * job_ptr IN - Job to test
 * test_only IN - If false, then attempt to allocate burst buffer if possible
 *
 * RET: 0 - stage-in is underway
 *      1 - stage-in complete
 *     -1 - stage-in not started or burst buffer in some unexpected state
 */
extern int bb_p_job_test_stage_in(struct job_record *job_ptr, bool test_only)
{ 
	debug2("RDEBUG : in bb_p_job_test_stage_in");
	return 1;
}

/* Attempt to claim burst buffer resources.
 * At this time, bb_g_job_test_stage_in() should have been run sucessfully AND
 * the compute nodes selected for the job.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_begin(struct job_record *job_ptr)
{
	int status = 0;
	char *rc_msg;
	char **script_argv;


	debug2("RDEBUG : bb_p_job_begin entry");
	/* step 1: start LOD */
	if (lustre_on_demand) {
		int index;

		debug2("RDEBUG: bb_p_job_begin found LD i.e. Lustre On Demand");

		script_argv = xmalloc(sizeof(char *) * 8);
		script_argv[0] = xstrdup("lod");
                index = 1;

                if (nodes != NULL) {
			xstrfmtcat(script_argv[index], "--node=%s", nodes);
			index ++;
		}

                if (mdtdevs != NULL) {
			xstrfmtcat(script_argv[index], "--mdtdevs=%s", mdtdevs);
			index ++;
		}
                if (ostdevs != NULL) {
			xstrfmtcat(script_argv[index], "--ostdevs=%s", ostdevs);
			index ++;
		}
                if (inet != NULL) {
			xstrfmtcat(script_argv[index], "--inet=%s", inet);
			index ++;
		}
                if (mountpoint != NULL) {
			xstrfmtcat(script_argv[index], "--mountpoint=%s", mountpoint);
			index ++;
		}
		script_argv[index] = xstrdup("start");

		rc_msg = run_command("lod_setup", "/usr/sbin/lod",
		                     script_argv, 8000000,
				     pthread_self(), &status);
		debug2("RDEBUG: command:");
		for (int i = 0; i <= index; i++)
			debug2("%s", script_argv[i]);
		debug2("RDEBUG: bb_p_job_begin lod_setup rc=[%s]", rc_msg);
		free_command_argv(script_argv);
		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return SLURM_ERROR;
		lod_started = true;
	}

	/* step 2: stage in */
	if (lod_stage_in) {
		lod_stage_in = false;
		script_argv = xmalloc(sizeof(char *) * 4);
		script_argv[0] = xstrdup("cp");
		xstrfmtcat(script_argv[1], "%s", sin_src);
		xstrfmtcat(script_argv[2], "%s", sin_dest);
		rc_msg = run_command("stage_in", "/usr/bin/cp", script_argv, 30000,
				     pthread_self(), &status);
		debug2("RDEBUG: bb_p_job_begin stage_in rc=[%s]", rc_msg);
		free_command_argv(script_argv);
		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

/* Revoke allocation, but do not release resources.
 * Executed after bb_p_job_begin() if there was an allocation failure.
 * Does not release previously allocated resources.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_revoke_alloc(struct job_record *job_ptr)
{
	debug2("RDEBUG : in bb_p_job_revoke_alloc");
	return SLURM_SUCCESS;
}

/*
 * Trigger a job's burst buffer stage-out to begin
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_start_stage_out(struct job_record *job_ptr)
{
	int status = 0;
	char *rc_msg;
	char **script_argv;

	debug2("RDEBUG: bb_p_job_start_stage_out entry");
	if (!lod_started) {
		debug2("RDEBUG: bb_p_job_start_stage_out lod no started.");
		return SLURM_ERROR;
	}

	/* step 1: stage out */
	if (lod_stage_out) {
		lod_stage_out = false;
		script_argv = xmalloc(sizeof(char *) * 4);
		script_argv[0] = xstrdup("cp");
		xstrfmtcat(script_argv[1], "%s", sout_src);
		xstrfmtcat(script_argv[2], "%s", sout_dest);
		script_argv[2] = xstrdup(sout_dest);
		rc_msg = run_command("stage_out", "/usr/bin/scp",
				     script_argv, 30000,
				     pthread_self(), &status);
		debug2("RDEBUG: bb_p_job_start_stage_out after stage_out rc=[%s]", rc_msg);
		free_command_argv(script_argv);
		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return SLURM_ERROR;
	}

	/* step 2: stop LOD */
	if (lod_started) {
                int index;

		script_argv = xmalloc(sizeof(char *) * 4);
		script_argv[0] = xstrdup("lod");
		index = 1;
                if (nodes != NULL) {
			xstrfmtcat(script_argv[index], "--node=%s", nodes);
			index ++;
		}

                if (mdtdevs != NULL) {
			xstrfmtcat(script_argv[index], "--mdtdevs=%s", mdtdevs);
			index ++;
		}
                if (ostdevs != NULL) {
			xstrfmtcat(script_argv[index], "--ostdevs=%s", ostdevs);
			index ++;
		}
                if (inet != NULL) {
			xstrfmtcat(script_argv[index], "--inet=%s", inet);
			index ++;
		}
                if (mountpoint != NULL) {
			xstrfmtcat(script_argv[index], "--mountpoint=%s", mountpoint);
			index ++;
		}
		script_argv[index] = xstrdup("stop");

		rc_msg = run_command("lod_teardown", "/usr/sbin/lod",
				     script_argv, 8000000,
				     pthread_self(), &status);
		debug2("RDEBUG: command:");
		for (int i = 0; i <= index; i++)
			debug2("%s", script_argv[i]);

		debug2("RDEBUG: bb_p_job_start_stage_out after lod_teardown rc=[%s]", rc_msg);
		free_command_argv(script_argv);
		lod_started = false;
		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return SLURM_ERROR;
	}

	debug2("RDEBUG: bb_p_job_start_stage_out return");
	return SLURM_SUCCESS;
}

/*
 * Determine if a job's burst buffer post_run operation is complete
 *
 * RET: 0 - post_run is underway
 *      1 - post_run complete
 *     -1 - fatal error
 */
extern int bb_p_job_test_post_run(struct job_record *job_ptr)
{
	debug2("RDEBUG : in bb_p_job_t_post_run");
	return 1;
}

/*
 * Determine if a job's burst buffer stage-out is complete
 *
 * RET: 0 - stage-out is underway
 *      1 - stage-out complete
 *     -1 - fatal error
 */
extern int bb_p_job_test_stage_out(struct job_record *job_ptr)
{
	debug2("RDEBUG : in bb_p_job_test_stage_out");
	return 1;
}

/*
 * Terminate any file staging and completely release burst buffer resources
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_cancel(struct job_record *job_ptr)
{
	debug2("RDEBUG : in bb_p_job_cancel");
	return SLURM_SUCCESS;
}

/*
 * Translate a burst buffer string to it's equivalent TRES string
 * Caller must xfree the return value
 */
extern char *bb_p_xlate_bb_2_tres_str(char *burst_buffer)
{
	debug2("RDEBUG : in bb_p_xlate_bb_2_tres_str");
	return NULL;
}
