/*****************************************************************************\
 *  burst_buffer_generic.c - Generic library for managing a burst_buffer
 *****************************************************************************
 *  Copyright (C) 2014-2015 SchedMD LLC.
 *  Written by Morris Jette <jette@schedmd.com>
 *  Copyright (c) 2018-2019 DataDirect Networks, Inc.
 *  Written by Gu Zheng <gzheng@ddn.com>
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

static bb_state_t	bb_state;

/* Script line types */
#define LINE_OTHER 0
#define LINE_LOD   1

typedef struct lod_bb_info {
	bool lod_started;
	bool lod_setup;
	bool lod_stage_out;
	bool lod_stage_in;
	bool lod_need_stop;

	/* LOD options */
	char *nodes;
	char *mdtdevs;
	char *ostdevs;
	char *inet;
	char *mountpoint;

	/* stage_in */
	char *sin_src;
	char *sin_srclist;
	char *sin_dest;

	/* stage_out */
	char *sout_src;
	char *sout_srclist;
	char *sout_dest;
} lod_bb_info_t;

static bb_job_t *_get_bb_job(struct job_record *job_ptr);
/* Validate that our configuration is valid for this plugin type */
static void _test_config(void)
{
	if (!bb_state.bb_config.get_sys_state) {
		debug("%s: GetSysState is NULL", __func__);
		bb_state.bb_config.get_sys_state =
			xstrdup("/usr/sbin/lod");
	}
}

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
extern int init(void)
{
	debug2("LOD_DEBUG : %s", __func__);
	slurm_mutex_init(&bb_state.bb_mutex);
	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_load_config(&bb_state, (char *)plugin_type); /* Removes "const" */
	_test_config();
	if (bb_state.bb_config.debug_flag)
		info("%s: %s", plugin_type,  __func__);
	bb_alloc_cache(&bb_state);
	slurm_mutex_unlock(&bb_state.bb_mutex);

	return SLURM_SUCCESS;
}

/*
 * fini() is called when the plugin is unloaded. Free all memory.
 */
extern int fini(void)
{
	int pc, last_pc = 0;

	debug2("LOD_DEBUG : %s", __func__);

	run_command_shutdown();
	while ((pc = run_command_count()) > 0) {
		if ((last_pc != 0) && (last_pc != pc)) {
			info("%s: waiting for %d running processes",
			     plugin_type, pc);
		}
		last_pc = pc;
		usleep(100000);
	}

	slurm_mutex_lock(&bb_state.bb_mutex);
	if (bb_state.bb_config.debug_flag)
		info("%s: %s", plugin_type,  __func__);

	slurm_mutex_lock(&bb_state.term_mutex);
	bb_state.term_flag = true;
	slurm_cond_signal(&bb_state.term_cond);
	slurm_mutex_unlock(&bb_state.term_mutex);

	bb_clear_config(&bb_state.bb_config, true);
	bb_clear_cache(&bb_state);
	slurm_mutex_unlock(&bb_state.bb_mutex);

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
	debug2("LOD_DEBUG : in bb_p_load_state");
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
	debug2("LOD_DEBUG : in bb_p_get_status");
	return NULL;
}

/*
 * Note configuration may have changed. Handle changes in BurstBufferParameters.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_reconfig(void)
{
	debug2("LOD_DEBUG : in bb_p_reconfig");
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
	debug2("LOD_DEBUG : in bb_p_state_pack");
	return SLURM_SUCCESS;
}

/* Copy a batch job's burst_buffer options into a separate buffer.
 * merge continued lines into a single line */
static int _xlate_batch(struct job_descriptor *job_desc)
{
	char *script, *save_ptr = NULL, *tok;
	int line_type;
	bool is_cont = false, has_space = false;
	int len, rc = SLURM_SUCCESS;

	script = xstrdup(job_desc->script);
	tok = strtok_r(script, "\n", &save_ptr);
	while (tok) {
		if (tok[0] != '#')
			break;	/* Quit at first non-comment */

		if ((tok[1] == 'L') && (tok[2] == 'O') && (tok[3] == 'D'))
			line_type = LINE_LOD;
		else
			line_type = LINE_OTHER;

		if (line_type == LINE_OTHER) {
			is_cont = false;
		} else {
			if (is_cont) {
				tok += 4; 	/* Skip "#LOD" */
				while (has_space && isspace(tok[0]))
					tok++;	/* Skip duplicate spaces */
			} else if (job_desc->burst_buffer) {
				xstrcat(job_desc->burst_buffer, "\n");
			}

			len = strlen(tok);
			if (tok[len - 1] == '\\') {
				has_space = isspace(tok[len - 2]);
				tok[strlen(tok) - 1] = '\0';
				is_cont = true;
			} else {
				is_cont = false;
			}
			xstrcat(job_desc->burst_buffer, tok);
		}
		tok = strtok_r(NULL, "\n", &save_ptr);
	}
	xfree(script);
	if (rc != SLURM_SUCCESS)
		xfree(job_desc->burst_buffer);
	return rc;
}

/* Return the burst buffer size specification of a job
 * RET size data structure or NULL of none found
 * NOTE: delete return value using _del_bb_size() */
static bb_job_t *_get_bb_job(struct job_record *job_ptr)
{
	bb_job_t *bb_job;

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return NULL;

	if ((bb_job = bb_job_find(&bb_state, job_ptr->job_id)))
		return bb_job;	/* Cached data */

	bb_job = bb_job_alloc(&bb_state, job_ptr->job_id);
	bb_job->account = xstrdup(job_ptr->account);
	if (job_ptr->part_ptr)
		bb_job->partition = xstrdup(job_ptr->part_ptr->name);
	if (job_ptr->qos_ptr)
		bb_job->qos = xstrdup(job_ptr->qos_ptr->name);
	bb_job->state = BB_STATE_PENDING;
	bb_job->user_id = job_ptr->user_id;

	return bb_job;
}

/* Perform basic burst_buffer option validation */
static int _parse_bb_opts(struct job_descriptor *job_desc)
{
	char *bb_script, *save_ptr = NULL;
	char *tok;
	bool lod_setup = false;
	bool lod_stop = false;
	bool lustre_on_demand = false;
	int rc = SLURM_SUCCESS;

	if (!job_desc->script)
		return rc;

	rc = _xlate_batch(job_desc);
	if (rc != SLURM_SUCCESS)
		return rc;

	if (job_desc->burst_buffer != NULL) {
		bb_script = xstrdup(job_desc->burst_buffer);
		tok = strtok_r(bb_script, "\n", &save_ptr);
		while (tok) {
			if (tok[0] != '#')
				break;  /* Quit at first non-comment */

			if ((tok[1] == 'L') && (tok[2] == 'O') && (tok[3] == 'D')) {
				lustre_on_demand = true;
				tok+=4;
			}

			if (lustre_on_demand) {
				while (isspace(tok[0]))
					tok++;
				/* setup can go with out options*/
				if (!strncmp(tok, "setup", 5)) {
					lod_setup = true;
					/* mdtdevs, ostdevs not specified? need lod.conf avalible */
					if (!strstr(tok, "mdtdevs=") || !strstr(tok, "ostdevs=")) {
						if (access("/etc/lod.conf", 0)) {
							error("%s: open access on config file /etc/lod.conf", __func__);
							return ESLURM_INVALID_BURST_BUFFER_REQUEST;
						}
					}
				} else if (!strncmp(tok, "stage_in", 8)) {
					if (!strstr(tok, "source=") || !strstr(tok, "destination=")) {
						error("%s: Stage-in requires source&destination", __func__);
						return ESLURM_INVALID_BURST_BUFFER_REQUEST;
					}
				} else if (!strncmp(tok, "stage_out", 8)) {
					if (!strstr(tok, "source=") || !strstr(tok, "destination=")) {
						error("%s: Stage-in requires source&destination", __func__);
						return ESLURM_INVALID_BURST_BUFFER_REQUEST;
					}
				} else if (!strncmp(tok, "stop", 4)) {
					lod_stop = true;
				}
			}
			tok = strtok_r(NULL, "\n", &save_ptr);
		}
		xfree(bb_script);
	}

	if (lod_stop && !lod_setup) {
		error("%s: Stop requires *setup*", __func__);
		rc = ESLURM_INVALID_BURST_BUFFER_REQUEST;
	}

	return rc;
}

/* Perform basic burst_buffer option validation */
static int _create_lod_job(struct job_record *job_ptr)
{
	char *bb_script, *save_ptr = NULL;
	char *tok;
	char *sub_tok;
	static bb_job_t *bb_job;
	bool lustre_on_demand = false;
	bool lod_started = false;
	bool lod_setup = false;
	bool lod_stage_out = false;
	bool lod_stage_in = false;
	bool lod_need_stop = false;

	/* LOD options */
	char *nodes = NULL;
	char *mdtdevs = NULL;
	char *ostdevs = NULL;
	char *inet= NULL;
	char *mountpoint = NULL;

	/* stage_in */
	char *sin_src = NULL;
	char *sin_srclist = NULL;
	char *sin_dest = NULL;

	/* stage_out */
	char *sout_src = NULL;
	char *sout_srclist = NULL;
	char *sout_dest = NULL;
	int rc = SLURM_SUCCESS;

	if (job_ptr->burst_buffer != NULL) {
		bb_script = xstrdup(job_ptr->burst_buffer);
		tok = strtok_r(bb_script, "\n", &save_ptr);
		bb_job = _get_bb_job(job_ptr);

		while (tok) {
			if (tok[0] != '#')
				break;  /* Quit at first non-comment */

			if ((tok[1] == 'L') && (tok[2] == 'O') && (tok[3] == 'D')) {
				lustre_on_demand = true;
				tok+=4;
				debug2("LOD_DEBUG: _parse_bb_opts found Lustre On Demand");
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
						debug2("LOD_DEBUG: _parse_bb_opts found node=%s", nodes);
					}

					if ((sub_tok = strstr(tok, "mdtdevs="))) {
						mdtdevs = xstrdup(sub_tok + 8);
						if ((sub_tok = strchr(mdtdevs, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found mdtdevs=%s", mdtdevs);
					}

					if ((sub_tok = strstr(tok, "ostdevs="))) {
						ostdevs = xstrdup(sub_tok + 8);
						if ((sub_tok = strchr(ostdevs, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found ostdevs=%s", ostdevs);
					}

					if ((sub_tok = strstr(tok, "inet="))) {
						inet = xstrdup(sub_tok + 5);
						if ((sub_tok = strchr(inet, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found inet=%s", inet);
					}

					if ((sub_tok = strstr(tok, "mountpoint="))) {
						mountpoint = xstrdup(sub_tok + 11);
						if ((sub_tok = strchr(mountpoint, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found mountpoint=%s", mountpoint);
					}
				} else if (!strncmp(tok, "stage_in", 8)) {
					xfree(sin_src);
					xfree(sin_srclist);
					xfree(sin_dest);
					lod_stage_in = true;
					debug2("LOD_DEBUG: _parse_bb_opts in stage_in");
					if ((sub_tok = strstr(tok, "source="))) {
						sin_src = xstrdup(sub_tok + 7);
						if ((sub_tok = strchr(sin_src, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sin_src=%s",
						       sin_src);
					}
					if ((sub_tok = strstr(tok, "sourcelist="))) {
						sin_srclist = xstrdup(sub_tok + 11);
						if ((sub_tok = strchr(sin_srclist, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sin_srclist=%s",
						       sin_srclist);
					}
					if ((sub_tok = strstr(tok, "destination="))) {
						sin_dest = xstrdup(sub_tok + 12);
						if ((sub_tok = strchr(sin_dest, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sin_dest=%s",
						       sin_dest);
					}
				} else if (!strncmp(tok, "stage_out", 8)) {
					xfree(sout_src);
					xfree(sout_srclist);
					xfree(sout_dest);
					lod_stage_out = true;
					debug2("LOD_DEBUG: _parse_bb_opts in stage_out");
					if ((sub_tok = strstr(tok, "source="))) {
						sout_src = xstrdup(sub_tok + 7);
						if ((sub_tok = strchr(sout_src, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sout_src=%s",
						       sout_src);
					}
					if ((sub_tok = strstr(tok, "sourcelist="))) {
						sout_srclist = xstrdup(sub_tok + 11);
						if ((sub_tok = strchr(sout_srclist, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sout_srclist=%s",
						       sout_srclist);
					}
					if ((sub_tok = strstr(tok, "destination="))) {
						sout_dest = xstrdup(sub_tok + 12);
						if ((sub_tok = strchr(sout_dest, ' ')))
							sub_tok[0] = '\0';
						debug2("LOD_DEBUG: _parse_bb_opts found sout_dest=%s",
						       sout_dest);
					}
				} else if (!strncmp(tok, "stop", 4)) {
					lod_need_stop = true;
					debug2("LOD_DEBUG: _parse_bb_opts in stage_out");
				}
			}

			tok = strtok_r(NULL, "\n", &save_ptr);
		}
		xfree(bb_script);
	} else {
		debug2("LOD_DEBUG: _parse_bb_opts no tok to parse");
	}

	if (lustre_on_demand) {
		bb_job->buf_ptr = xmalloc(sizeof(bb_buf_t));
		lod_bb_info_t *lod_bb = xmalloc(sizeof(lod_bb_info_t));
		/* hack bb_buf_t->access to store lod_bb_info_t*/
		lod_bb->lod_started = lod_started;
		lod_bb->lod_setup = lod_setup;
		lod_bb->lod_stage_out = lod_stage_out;
		lod_bb->lod_stage_in = lod_stage_in;
		lod_bb->lod_need_stop = lod_need_stop;

		/* LOD options */
		lod_bb->nodes = nodes;
		lod_bb->mdtdevs = mdtdevs;
		lod_bb->ostdevs = ostdevs;
		lod_bb->inet = inet;
		lod_bb->mountpoint = mountpoint;

		/* stage_in */
		lod_bb->sin_src = sin_src;
		lod_bb->sin_srclist = sin_srclist;
		lod_bb->sin_dest = sin_dest;

		/* stage_out */
		lod_bb->sout_src = sout_src;
		lod_bb->sout_srclist = sout_srclist;
		lod_bb->sout_dest = sout_dest;
		bb_job->buf_ptr->access = lod_bb;
	}

	debug2("LOD_DEBUG: _parse_bb_opts return");
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
	int rc = SLURM_SUCCESS;

	xassert(job_desc);
	xassert(job_desc->tres_req_cnt);

	debug2("LOD_DEBUG : in bb_p_job_validate before parsing");

	rc = _parse_bb_opts(job_desc);
	if (rc != SLURM_SUCCESS)
		goto out;

	if ((job_desc->burst_buffer == NULL) ||
	    (job_desc->burst_buffer[0] == '\0'))
		return rc;

	info("%s: %s: job_user_id:%u, submit_uid:%d",
	     plugin_type, __func__, job_desc->user_id, submit_uid);
	info("%s: burst_buffer:\n%s", __func__, job_desc->burst_buffer);

out:
	debug2("LOD_DEBUG : in bb_p_job_validate after parsing");
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
	debug2("LOD_DEBUG : in bb_p_job_validate2");

        int rc = _create_lod_job(job_ptr);
        
	return rc;
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
	debug2("LOD_DEBUG : in bb_p_job_set_tres_cnt");
}

/*
 * For a given job, return our best guess if when it might be able to start
 */
extern time_t bb_p_job_get_est_start(struct job_record *job_ptr)
{
	time_t est_start = time(NULL);
	return est_start;
}

static void* _start_stage_in(void *ptr);
static void _job_queue_del(void *x)
{
	bb_job_queue_rec_t *job_rec = (bb_job_queue_rec_t *) x;
	if (job_rec) {
		xfree(job_rec);
	}
}
/*
 * Attempt to allocate resources and begin file staging for pending jobs.
 */
extern int bb_p_job_try_stage_in(List job_queue)
{
	pthread_t tid;
	bb_job_queue_rec_t *job_rec;
	List job_candidates;
	ListIterator job_iter;
	struct job_record *job_ptr;
	bb_job_t *bb_job;

	debug2("LOD_DEBUG :entry bb_p_job_try_stage_in");
	slurm_mutex_lock(&bb_state.bb_mutex);
	if (bb_state.bb_config.debug_flag)
		info("%s: %s", plugin_type,  __func__);

	/* Identify candidates to be allocated burst buffers */
	job_candidates = list_create(_job_queue_del);

	job_iter = list_iterator_create(job_queue);
	while ((job_ptr = list_next(job_iter))) {
		if (!IS_JOB_PENDING(job_ptr) ||
		    (job_ptr->start_time == 0) ||
		    (job_ptr->burst_buffer == NULL) ||
		    (job_ptr->burst_buffer[0] == '\0'))
			continue;
		if (job_ptr->array_recs &&
		    ((job_ptr->array_task_id == NO_VAL) ||
		     (job_ptr->array_task_id == INFINITE)))
			continue;	/* Can't operate on job array struct */

		bb_job = bb_job_find(&bb_state, job_ptr->job_id);
		if (bb_job == NULL)
			continue;

		if (bb_job->state == BB_STATE_COMPLETE)
			bb_job->state = BB_STATE_PENDING;     /* job requeued */
		else if (bb_job->state >= BB_STATE_POST_RUN)
			continue;	/* Requeued job still staging out */

		if (bb_job->state >= BB_STATE_STAGING_IN)
			continue;	/* Job was already allocated a buffer */

		//slurm_thread_create(&tid, _start_stage_in, job_ptr);

		job_rec = xmalloc(sizeof(bb_job_queue_rec_t));
		job_rec->job_ptr = job_ptr;
		job_rec->bb_job = bb_job;
		list_push(job_candidates, job_rec);
	}
	list_iterator_destroy(job_iter);
	/* Sort in order of expected start time */
	list_sort(job_candidates, bb_job_queue_sort);

	bb_set_use_time(&bb_state);
	job_iter = list_iterator_create(job_candidates);
	while ((job_rec = list_next(job_iter))) {
		job_ptr = job_rec->job_ptr;
		bb_job = job_rec->bb_job;
		if (bb_job->state >= BB_STATE_STAGING_IN)
			continue;	/* Job was already allocated a buffer */
		//slurm_thread_create_detached(&tid, _start_stage_in, job_ptr);
		slurm_thread_create(&tid, _start_stage_in, job_ptr);
	}
	slurm_mutex_unlock(&bb_state.bb_mutex);
	list_iterator_destroy(job_iter);

	FREE_NULL_LIST(job_candidates);
	debug2("LOD_DEBUG :exit bb_p_job_try_stage_in");
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
        bb_job_t *bb_job;
	int rc;
	debug2("LOD_DEBUG : in bb_p_job_test_stage_in");

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return 1;

	bb_job = bb_job_find(&bb_state, job_ptr->job_id);
	if (!bb_job) {
		/* No job buffers. Assuming use of persistent buffers only */
		debug2("%s: %pJ bb job record not found", __func__, job_ptr);
		rc =  -1;
	} else {
		if (bb_job->state <= BB_STATE_STAGING_IN) {
			rc = 0;
		} else if (bb_job->state >= BB_STATE_STAGED_IN) {
			rc =  1;
		} else {
			rc = -1;
		}
	}

	debug2("LOD_DEBUG : out bb_p_job_test_stage_in: state:%d rc:%d", bb_job->state, rc);

	return rc;
}

static void* _start_stage_in(void *ptr)
{
	int status;
	struct job_record *job_ptr = (struct job_record *)ptr;
	uint32_t job_id = job_ptr->job_id;
	uint32_t timeout;
	int index;
        bb_job_t *bb_job;
	lod_bb_info_t *lod_bb;
	DEF_TIMERS;

	char *rc_msg;
	char **script_argv;

	debug2("LOD_DEBUG : %s entry", __func__);

	track_script_rec_t *track_script_rec =
		track_script_rec_add(job_id, 0, pthread_self());

	job_ptr = find_job_record(job_id);
	bb_job = bb_job_find(&bb_state, job_id);
	if (!job_ptr) {
		error("%s: unable to find job record for JobId=%u",
		      __func__, job_id);
		return NULL;
	} else if (!bb_job) {
		error("%s: unable to find bb_job record for %pJ",
		      __func__, job_ptr);
		return NULL;
	}

	lod_bb = (lod_bb_info_t *)bb_job->buf_ptr->access;
	bb_job->state = BB_STATE_STAGING_IN;

	if (bb_state.bb_config.other_timeout)
		timeout = bb_state.bb_config.other_timeout * 1000;
	else
		timeout = DEFAULT_OTHER_TIMEOUT * 1000;

	if (lod_bb->lod_setup) {
		/* step 1: start LOD */
		debug2("LOD_DEBUG: _start_stage_in found LOD i.e. Lustre On Demand");

		bb_job = bb_job_find(&bb_state, job_id);

		script_argv = xcalloc(12, sizeof(char *));
		script_argv[0] = xstrdup("lod");
                index = 1;
                if (lod_bb->nodes != NULL) {
			xstrfmtcat(script_argv[index], "--node=%s",
				   lod_bb->nodes);
			index ++;
		} else if (job_ptr->details->req_nodes) {
			char *req_nodes;

			req_nodes = xstrdup(job_ptr->details->req_nodes);
			xstrfmtcat(script_argv[index], "--node=%s", req_nodes);
			xfree(req_nodes);
			index ++;
		}

                if (lod_bb->mdtdevs != NULL) {
			xstrfmtcat(script_argv[index], "--mdtdevs=%s",
				   lod_bb->mdtdevs);
			index ++;
		}
                if (lod_bb->ostdevs != NULL) {
			xstrfmtcat(script_argv[index], "--ostdevs=%s",
				   lod_bb->ostdevs);
			index ++;
		}
                if (lod_bb->inet != NULL) {
			xstrfmtcat(script_argv[index], "--inet=%s",
				   lod_bb->inet);
			index ++;
		}
                if (lod_bb->mountpoint != NULL) {
			xstrfmtcat(script_argv[index], "--mountpoint=%s", lod_bb->mountpoint);
			index ++;
		}
		script_argv[index] = xstrdup("start");

		debug2("LOD_DEBUG: command:");
		for (int i = 0; i <= index; i++)
			debug2("%s", script_argv[i]);

		START_TIMER;
		rc_msg = run_command("lod_setup", bb_state.bb_config.get_sys_state,
		                     script_argv, timeout,
				     pthread_self(), &status);

		debug2("LOD_DEBUG: bb_p_job_begin lod_setup rc=[%s]", rc_msg);
		END_TIMER;
		info("%s: setup for job JobId=%u ran for %s",
		     __func__, job_id, TIME_STR);
		free_command_argv(script_argv);
		if (track_script_broadcast(track_script_rec, status)) {
			/* I was killed by slurmtrack, bail out right now */
			info("%s: setup for JobId=%u terminated by slurmctld",
			     __func__, job_id);
			/*
			 * Don't need to free track_script_rec here,
			 * it is handled elsewhere since it still being tracked.
			 */
			return NULL;
		}
		track_script_reset_cpid(pthread_self(), 0);

		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return NULL;
		bb_job = bb_job_find(&bb_state, job_id);
		lod_bb->lod_started = true;
		bb_job->state = BB_STATE_STAGING_IN;
	}

	/* step 2: stage in */
	if (lod_bb->lod_stage_in) {
		script_argv = xcalloc(12, sizeof(char *));
		script_argv[0] = xstrdup("lod");
                index = 1;

                if (lod_bb->nodes != NULL) {
			xstrfmtcat(script_argv[index], "--node=%s",
				   lod_bb->nodes);
			index ++;
		} else if (job_ptr->details->req_nodes) {
			char *req_nodes;

			req_nodes = xstrdup(job_ptr->details->req_nodes);
			xstrfmtcat(script_argv[index], "--node=%s", req_nodes);
			xfree(req_nodes);
			index ++;
		}

                if (lod_bb->mdtdevs != NULL) {
			xstrfmtcat(script_argv[index], "--mdtdevs=%s",
				   lod_bb->mdtdevs);
			index ++;
		}
                if (lod_bb->ostdevs != NULL) {
			xstrfmtcat(script_argv[index], "--ostdevs=%s",
				   lod_bb->ostdevs);
			index ++;
		}
                if (lod_bb->inet != NULL) {
			xstrfmtcat(script_argv[index], "--inet=%s",
				   lod_bb->inet);
			index ++;
		}
                if (lod_bb->mountpoint != NULL) {
			xstrfmtcat(script_argv[index], "--mountpoint=%s",
			lod_bb->mountpoint);
			index ++;
		}
                if (lod_bb->sin_src != NULL) {
			xstrfmtcat(script_argv[index], "--source=%s",
				   lod_bb->sin_src);
			index ++;
		}
                if (lod_bb->sin_srclist != NULL) {
			xstrfmtcat(script_argv[index], "--sourcelist=%s",
				   lod_bb->sin_srclist);
			index ++;
		}
                if (lod_bb->sin_dest != NULL) {
			xstrfmtcat(script_argv[index], "--destination=%s",
				   lod_bb->sin_dest);
			index ++;
		}

		script_argv[index] = xstrdup("stage_in");

		debug2("LOD_DEBUG: command:");
		for (int i = 0; i <= index; i++)
			debug2("%s", script_argv[i]);
		START_TIMER;
		rc_msg = run_command("stage_in",
				     bb_state.bb_config.get_sys_state,
				     script_argv, timeout,
				     pthread_self(), &status);
		END_TIMER;
		free_command_argv(script_argv);

		if (track_script_broadcast(track_script_rec, status)) {
			/* I was killed by slurmtrack, bail out right now */
			info("%s: stage_in for JobId=%u terminated by slurmctld",
			     __func__, job_id);
			/*
			 * Don't need to free track_script_rec here,
			 * it is handled elsewhere since it still being tracked.
			 */
			return NULL;
		}
		track_script_reset_cpid(pthread_self(), 0);
		if (bb_state.bb_config.debug_flag)
			info("%s: stage_in ran for %s", __func__, TIME_STR);

		debug2("LOD_DEBUG: bb_p_job_begin stage_in rc=[%s]", rc_msg);

		if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0))
			return NULL;
	}
	bb_job = bb_job_find(&bb_state, job_id);
	bb_job->state = BB_STATE_STAGED_IN;

	job_ptr = find_job_record(job_id);
	if (!job_ptr) {
		error("%s: unable to find job record for JobId=%u",
		      __func__, job_id);
	} else {
		/* stage in completet, kick up job */
		queue_job_scheduler();
		bb_state.last_update_time = time(NULL);
	}

	track_script_remove(pthread_self());

	return NULL;
}

/* Attempt to claim burst buffer resources.
 * At this time, bb_g_job_test_stage_in() should have been run sucessfully AND
 * the compute nodes selected for the job.
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_begin(struct job_record *job_ptr)
{
	bb_job_t *bb_job;

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return SLURM_SUCCESS;

	slurm_mutex_lock(&bb_state.bb_mutex);
	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		error("%s: %s: no job record buffer for %pJ",
		      plugin_type, __func__, job_ptr);
		xfree(job_ptr->state_desc);
		job_ptr->state_desc =
			xstrdup("Could not find burst buffer record");
		job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
		slurm_mutex_unlock(&bb_state.bb_mutex);
		return SLURM_ERROR;
	}
	bb_job->state = BB_STATE_RUNNING;
	slurm_mutex_unlock(&bb_state.bb_mutex);

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
	debug2("LOD_DEBUG : in bb_p_job_revoke_alloc");
	return SLURM_SUCCESS;
}

static void* _start_teardown(void *data) {
	int index, status, rc = SLURM_SUCCESS;
	bb_job_t *bb_job = (bb_job_t *)data;
	struct job_record *job_ptr;
	lod_bb_info_t *lod_bb;
	uint32_t timeout;
	DEF_TIMERS
	track_script_rec_t *track_script_rec;
	char *rc_msg;
	char **script_argv;

	lod_bb = (lod_bb_info_t *)bb_job->buf_ptr->access;
	debug2("LOD_DEBUG : %s entry, lod_bb->lod_setup:%d,lod_bb->lod_started:%d,lod_bb->lod_need_stop:%d", __func__, lod_bb->lod_setup, lod_bb->lod_started,lod_bb->lod_need_stop);
	if (!lod_bb->lod_setup || !lod_bb->lod_started || !lod_bb->lod_need_stop)
		return NULL;

	if (bb_state.bb_config.other_timeout)
		timeout = bb_state.bb_config.other_timeout * 1000;
	else
		timeout = DEFAULT_OTHER_TIMEOUT * 1000;

	job_ptr = find_job_record(bb_job->job_id);
	track_script_rec = track_script_rec_add(job_ptr->job_id, 0, pthread_self());
	script_argv = xcalloc(8, sizeof(char *));
	script_argv[0] = xstrdup("lod");
        index = 1;

        if (lod_bb->nodes != NULL) {
		xstrfmtcat(script_argv[index], "--node=%s",
			   lod_bb->nodes);
		index ++;
	} else if (job_ptr->job_resrcs->nodes) {
		char *res_nodes;

		res_nodes = xstrdup(job_ptr->job_resrcs->nodes);
		xstrfmtcat(script_argv[index], "--node=%s", res_nodes);
		xfree(res_nodes);
		index ++;
	}

        if (lod_bb->mdtdevs != NULL) {
		xstrfmtcat(script_argv[index], "--mdtdevs=%s",
			   lod_bb->mdtdevs);
		index ++;
	}
        if (lod_bb->ostdevs != NULL) {
		xstrfmtcat(script_argv[index], "--ostdevs=%s",
			   lod_bb->ostdevs);
		index ++;
	}
        if (lod_bb->inet != NULL) {
		xstrfmtcat(script_argv[index], "--inet=%s",
			   lod_bb->inet);
		index ++;
	}
        if (lod_bb->mountpoint != NULL) {
		xstrfmtcat(script_argv[index], "--mountpoint=%s",
			   lod_bb->mountpoint);
		index ++;
	}

	script_argv[index] = xstrdup("stop");

	debug2("LOD_DEBUG: command:");
	for (int i = 0; i <= index; i++)
		debug2("%s", script_argv[i]);
	START_TIMER;
	rc_msg = run_command("teardown",
			     bb_state.bb_config.get_sys_state,
			     script_argv, timeout,
			     pthread_self(), &status);
	END_TIMER;
	info("%s: setup for job JobId=%u ran for %s",
	     __func__, bb_job->job_id, TIME_STR);
	free_command_argv(script_argv);

	debug2("LOD_DEBUG: %s after teardown rc=[%s]", __func__, rc_msg);
	if (track_script_broadcast(track_script_rec, status)) {
		/* I was killed by slurmtrack, bail out right now */
		info("%s: teardown for JobId=%u terminated by slurmctld",
		     __func__, bb_job->job_id);
		return NULL;
	}
	track_script_reset_cpid(pthread_self(), 0);

	job_ptr = find_job_record(bb_job->job_id);
	bb_job = _get_bb_job(job_ptr);

	if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0)) {
		error("%s: teardown for JobId=%u status:%u response:%s",
		      __func__, bb_job->job_id, status, rc_msg);
		rc = SLURM_ERROR;
		if (job_ptr) {
			job_ptr->state_reason = BB_STATE_TEARDOWN_FAIL;
			xfree(job_ptr->state_desc);
			xstrfmtcat(job_ptr->state_desc, "%s: teardown: %s",
				   plugin_type, rc_msg);
		}
	}

	if (!job_ptr) {
		error("%s: unable to find job record for JobId=%u",
		      __func__, bb_job->job_id);
	} else if (rc == SLURM_SUCCESS) {
		bb_job->state = BB_STATE_COMPLETE;
	} else {
		bb_job->state = BB_STATE_TEARDOWN_FAIL;
	}

	xfree(job_ptr->state_desc);
	job_ptr->job_state &= (~JOB_STAGE_OUT);

	track_script_remove(pthread_self());
	return NULL;
}

static void *_start_stage_out(void *data) {
	int index, status, rc = SLURM_SUCCESS;
	bb_job_t *bb_job = (bb_job_t *)data;
	struct job_record *job_ptr;
	lod_bb_info_t *lod_bb = (lod_bb_info_t *)bb_job->buf_ptr->access;;
	uint32_t timeout;
	DEF_TIMERS
	track_script_rec_t *track_script_rec;
	char *rc_msg;
	char **script_argv;

	debug2("LOD_DEBUG : %s entry", __func__);
        if (!lod_bb->lod_stage_in || !lod_bb->lod_stage_out || !lod_bb->lod_started)
		return NULL;

	if (bb_state.bb_config.other_timeout)
		timeout = bb_state.bb_config.other_timeout * 1000;
	else
		timeout = DEFAULT_OTHER_TIMEOUT * 1000;

	script_argv = xcalloc(10, sizeof(char *));
	track_script_rec = track_script_rec_add(bb_job->job_id, 0, pthread_self());
	script_argv[0] = xstrdup("lod");
        index = 1;

	job_ptr = find_job_record(bb_job->job_id);
        if (lod_bb->nodes != NULL) {
		xstrfmtcat(script_argv[index], "--node=%s",
			   lod_bb->nodes);
		index ++;
	} else if (job_ptr->job_resrcs->nodes) {
		char *res_nodes;

		res_nodes = xstrdup(job_ptr->job_resrcs->nodes);
		xstrfmtcat(script_argv[index], "--node=%s", res_nodes);
		xfree(res_nodes);
		index ++;
	}

        if (lod_bb->mdtdevs != NULL) {
		xstrfmtcat(script_argv[index], "--mdtdevs=%s",
			   lod_bb->mdtdevs);
		index ++;
	}
        if (lod_bb->ostdevs != NULL) {
		xstrfmtcat(script_argv[index], "--ostdevs=%s",
			   lod_bb->ostdevs);
		index ++;
	}
        if (lod_bb->inet != NULL) {
		xstrfmtcat(script_argv[index], "--inet=%s",
			   lod_bb->inet);
		index ++;
	}
        if (lod_bb->mountpoint != NULL) {
		xstrfmtcat(script_argv[index], "--mountpoint=%s",
			   lod_bb->mountpoint);
		index ++;
	}
	if (lod_bb->sout_srclist != NULL) {
		xstrfmtcat(script_argv[index], "--sourcelist=%s",
			   lod_bb->sout_srclist);
		index ++;
	}
        if (lod_bb->sout_src != NULL) {
		xstrfmtcat(script_argv[index], "--source=%s",
			   lod_bb->sout_src);
		index ++;
	}
        if (lod_bb->sout_dest != NULL) {
		xstrfmtcat(script_argv[index], "--destination=%s",
			   lod_bb->sout_dest);
		index ++;
	}

	script_argv[index] = xstrdup("stage_out");

	debug2("LOD_DEBUG: command:");
	for (int i = 0; i <= index; i++)
		debug2("%s", script_argv[i]);
	START_TIMER;
	rc_msg = run_command("stage_out",
			     bb_state.bb_config.get_sys_state,
			     script_argv, timeout,
			     pthread_self(), &status);
	END_TIMER;
	info("%s: setup for job JobId=%u ran for %s",
	     __func__, bb_job->job_id, TIME_STR);
	free_command_argv(script_argv);

	debug2("LOD_DEBUG: bb_p_job_start_stage_out after stage_out rc=[%s]", rc_msg);
	if (track_script_broadcast(track_script_rec, status)) {
		/* I was killed by slurmtrack, bail out right now */
		info("%s: stage_out for JobId=%u terminated by slurmctld",
		     __func__, bb_job->job_id);
		return NULL;
	}
	track_script_reset_cpid(pthread_self(), 0);

	job_ptr = find_job_record(bb_job->job_id);

	if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0)) {
		error("%s: post_run for JobId=%u status:%u response:%s",
		      __func__, bb_job->job_id, status, rc_msg);
		rc = SLURM_ERROR;
		if (job_ptr) {
			job_ptr->state_reason = FAIL_BURST_BUFFER_OP;
			xfree(job_ptr->state_desc);
			xstrfmtcat(job_ptr->state_desc, "%s: post_run: %s",
				   plugin_type, rc_msg);
		}
	}

	track_script_remove(pthread_self());
	if (!job_ptr) {
		error("%s: unable to find job record for JobId=%u",
		      __func__, bb_job->job_id);
	} else if (rc == SLURM_SUCCESS) {
		bb_job->state = BB_STATE_STAGED_OUT;
		if (lod_bb->lod_stage_out)
			_start_teardown((void *)bb_job);
	}

	return NULL;
}

/*
 * Trigger a job's burst buffer stage-out to begin
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_start_stage_out(struct job_record *job_ptr)
{
	pthread_t tid;
        bb_job_t *bb_job;
	lod_bb_info_t *lod_bb;

	debug2("LOD_DEBUG : %s entry", __func__);

	if (bb_state.bb_config.debug_flag)
		info("%s: %s: %pJ", plugin_type, __func__, job_ptr);

	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		/* No job buffers. Assuming use of persistent buffers only */
		verbose("%s: %pJ bb job record not found", __func__, job_ptr);
		goto out;
	}

	lod_bb = (lod_bb_info_t *)bb_job->buf_ptr->access;
	if (bb_job->state < BB_STATE_RUNNING || !lod_bb->lod_stage_out) {
		/* Job never started or no stage_out. Just teardown the buffer */
		bb_job->state = BB_STATE_TEARDOWN;
		//slurm_thread_create_detached(NULL, _start_teardown, (void *)bb_job);
		slurm_thread_create(&tid, _start_teardown, (void *)bb_job);
	} else if (bb_job->state < BB_STATE_POST_RUN) {
		bb_job->state = BB_STATE_POST_RUN;
		job_ptr->job_state |= JOB_STAGE_OUT;
		xfree(job_ptr->state_desc);
		xstrfmtcat(job_ptr->state_desc, "%s: Stage-out in progress",
			   plugin_type);
		//slurm_thread_create_detached(NULL, _start_stage_out, (void *)bb_job);
		slurm_thread_create(&tid, _start_stage_out, (void *)bb_job);
	}
out:
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
	debug2("LOD_DEBUG : in bb_p_job_t_post_run");

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
	int rc;
        bb_job_t *bb_job;
	debug2("LOD_DEBUG : in bb_p_job_test_stage_out");

	if ((job_ptr->burst_buffer == NULL) ||
	    (job_ptr->burst_buffer[0] == '\0'))
		return 1;

	bb_job = bb_job_find(&bb_state, job_ptr->job_id);
	if (!bb_job) {
		/* No job buffers. Assuming use of persistent buffers only */
		verbose("%s: %pJ bb job record not found", __func__, job_ptr);
		rc =  1;
	} else {
		if (bb_job->state == BB_STATE_PENDING) {
			/*
			 * No job BB work not started before job was killed.
			 * Alternately slurmctld daemon restarted after the
			 * job's BB work was completed.
			 */
			rc =  1;
		} else if (bb_job->state < BB_STATE_POST_RUN) {
			rc = -1;
		} else if (bb_job->state > BB_STATE_STAGING_OUT) {
			rc =  1;
		} else {
			rc =  0;
		}
	}

	return rc;
	return 1;
}

/*
 * Terminate any file staging and completely release burst buffer resources
 *
 * Returns a Slurm errno.
 */
extern int bb_p_job_cancel(struct job_record *job_ptr)
{
	pthread_t tid;
        bb_job_t *bb_job;

	debug2("LOD_DEBUG : %s entry", __func__);

	if (bb_state.bb_config.debug_flag)
		info("%s: %s: %pJ", plugin_type, __func__, job_ptr);

	bb_job = _get_bb_job(job_ptr);
	if (!bb_job) {
		verbose("%s: %pJ bb job record not found", __func__, job_ptr);
		goto out;
	}

	if (bb_job->state == BB_STATE_PENDING) {
		bb_job->state = BB_STATE_COMPLETE;  /* Nothing to clean up */
	} else if (bb_job->state < BB_STATE_POST_RUN) {
		bb_job->state = BB_STATE_TEARDOWN;
		slurm_thread_create(&tid, _start_teardown, (void *)bb_job);
	}
out:
	return SLURM_SUCCESS;
}

/*
 * Translate a burst buffer string to it's equivalent TRES string
 * Caller must xfree the return value
 */
extern char *bb_p_xlate_bb_2_tres_str(char *burst_buffer)
{
	debug2("LOD_DEBUG : in bb_p_xlate_bb_2_tres_str");
	return NULL;
}
