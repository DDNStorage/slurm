/*****************************************************************************\
 *  src/slurmd/elan_interconnect.c Elan interconnect implementation
 *****************************************************************************
 *  Copyright (C) 2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Kevin Tew <tew1@llnl.gov> 
 *         and Mark Grondona <mgrondona@llnl.gov>
 *  UCRL-CODE-2002-040.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *  
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
\*****************************************************************************/

#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include <slurm/slurm_errno.h>

#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/bitstring.h"
#include "src/common/log.h"
#include "src/common/qsw.h"
#include "src/common/slurm_protocol_api.h"
#include "src/slurmd/interconnect.h"
#include "src/slurmd/setenvpf.h"
#include "src/slurmd/shm.h"

static int 
_wait_and_destroy_prg(qsw_jobinfo_t qsw_job, pid_t pid)
{
	int i = 0;
	int sleeptime = 1;

	debug3("waiting to destory program description...");
  again:
	if (waitpid(pid, NULL, 0) < 0) {
		if (errno == EINTR)
			goto again;
		error("waitpid: %m");
		exit(1);
	}

	while(qsw_prgdestroy(qsw_job) < 0) {
		i++;
		error("qsw_prgdestroy: %m");
		if (errno == ESRCH)
			break;
		if (i == 1) {
			debug("sending SIGTERM to remaining tasks");
			qsw_prgsignal(qsw_job, SIGTERM);
		} else {
			debug("sending SIGKILL to remaining tasks");
			qsw_prgsignal(qsw_job, SIGKILL);
		}

		debug("going to sleep for %d seconds and try again", sleeptime);
		sleep(sleeptime*=2);
	}

	debug("destroyed program description");

	exit(0);
	return SLURM_SUCCESS;
}

/* 
 * prepare node for interconnect use
 */
int 
interconnect_init(slurmd_job_t *job)
{
	pid_t pid;

	/* Process 1: */
	switch ((pid = fork())) 
	{
		case -1:
			error ("elan_interconnect_prepare fork(): %m");
			return SLURM_ERROR ;
		case 0: /* child falls thru */
			break;
		default: /* parent */
			_wait_and_destroy_prg(job->qsw_job, pid);
			/*NOTREACHED*/
	}

	/* Process 2: */
	debug("calling qsw_prog_init from process %ld", getpid());
	if (qsw_prog_init(job->qsw_job, job->uid) < 0) {
		error ("elan interconnect_init: qsw_prog_init: %m");
		/* we may lose the following info if not logging to stderr */
		qsw_print_jobinfo(stderr, job->qsw_job);
		return SLURM_ERROR;
	}
	
	return SLURM_SUCCESS; 
}

int 
interconnect_fini(slurmd_job_t *job)
{
	return SLURM_SUCCESS;
}
int 
interconnect_attach(slurmd_job_t *job, int procid)
{
	int nodeid, nnodes, nprocs; 

	nodeid = job->nodeid;
	nnodes = job->nnodes;
	nprocs = job->nprocs;

	debug3("nodeid=%d nnodes=%d procid=%d nprocs=%d", 
	       nodeid, nnodes, procid, nprocs);
	debug3("setting capability in process %ld", getpid());
	if (qsw_setcap(job->qsw_job, procid) < 0) {
		error("qsw_setcap: %m");
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

/*
 * Set environment variables needed by QSW MPICH / libelan.
 */
int interconnect_env(slurmd_job_t *job, int taskid)
{
	int cnt  = job->envc;
	int rank = job->task[taskid]->gid; 

	if (setenvpf(&job->env, &cnt, "RMS_RANK=%d",   rank       ) < 0)
		return -1;
	if (setenvpf(&job->env, &cnt, "RMS_NODEID=%d", job->nodeid) < 0)
		return -1;
	if (setenvpf(&job->env, &cnt, "RMS_PROCID=%d", rank       ) < 0)
		return -1;
	if (setenvpf(&job->env, &cnt, "RMS_NNODES=%d", job->nnodes) < 0)
		return -1;
	if (setenvpf(&job->env, &cnt, "RMS_NPROCS=%d", job->nprocs) < 0)
		return -1;
	return 0;
}

