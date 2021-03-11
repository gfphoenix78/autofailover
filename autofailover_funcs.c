#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "replication/walsender_private.h"
#include "replication/walreceiver.h"

#include <unistd.h>

PG_MODULE_MAGIC;


// probe:
// 	current_role: primary/secondary
// 	expires:      when primary is valid
// 	lsn:          latest LSN synchronized to the secondary
//
// tuple probe()
//
// bool set_sync(string val);
// bool promote();
//


PG_FUNCTION_INFO_V1(collect_status);
PG_FUNCTION_INFO_V1(execute_action);

extern int max_wal_senders;
// (last_role) => (running_role, sync, lsn, walconn)
// role:
// 	p: un-acked primary
// 	s: un-acked secondary
// 	P: acked primary
// 	S: acked secondary
// sync: 't', 'f', '?'
// 	primary: sync with at least one secondary
// 	secondary: sync to the peer
// walconn: wal connection is healthy

static char probe_role()
{
//  ControlFileData *control_file;
  int fd;
//  bool found;
  fd = open("standby.signal", O_RDONLY);
  if (fd >= 0)
  {
    close(fd);
    return 's';
  }
  if (errno != ENOENT)
    ereport(ERROR, (errmsg("open standby.signal: %m")));
//
//  control_file = (ControlFileData *)ShmemInitStruct("Control File",
//  					sizeof(ControlFileData), &found);
//  Assert(found && "found control file");
//  if (control_file->state == DB_IN_PRODUCTION)
  return 'p';
}

static Datum char_to_text_datum(char ch)
{
	char b[2];
	b[0] = ch;
	b[1] = '\0';
	return CStringGetTextDatum(b);
}

static void check_syncrep()
{
	if (SyncRepStandbyNames == NULL || SyncRepStandbyNames[0] == '\0')
		ereport(ERROR, (errmsg("synchronous_standby_names is not defined")));
}

static XLogRecPtr
get_current_lsn()
{
	XLogRecPtr lsn;
	if (RecoveryInProgress())
	{
		lsn = GetWalRcvWriteRecPtr(NULL, NULL);
		if (lsn == 0)
			lsn = GetXLogReplayRecPtr(NULL);
	}
	else
	{
		lsn = GetFlushRecPtr();
	}
	return lsn;
}

Datum
collect_status(PG_FUNCTION_ARGS)
{
	char role;
	bool walconn = false;
	const char *sync = "?";
	XLogRecPtr lsn = 0;

	TupleDesc desc;
	TypeFuncClass funcClass;
	HeapTuple tuple;
	Datum resultDatum;
	Datum values[4];
	bool isNulls[4];

	funcClass = get_call_result_type(fcinfo, NULL, &desc);
	if (funcClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("return type must be a row type")));
	}

	memset(values, 0, sizeof(values));
	memset(isNulls, 0, sizeof(isNulls));

	role = probe_role();

	if (role == 'p')
	{
		int i;
		for (i = 0; i < max_wal_senders; i++)
		{
			WalSnd *walsnd = &WalSndCtl->walsnds[i];
			WalSndState state;
			if (walsnd->pid == 0)
				continue;
			SpinLockAcquire(&walsnd->mutex);
			state = walsnd->state;
			SpinLockRelease(&walsnd->mutex);
			switch (state)
			{
				case WALSNDSTATE_STREAMING:
				case WALSNDSTATE_CATCHUP:
					sync = state == WALSNDSTATE_STREAMING
								? "streaming" : "catchup";
					walconn = true;
					break;
				default:
					break;
			}
			if (state == WALSNDSTATE_STREAMING)
				break;
		}
	}
	else
	{
		WalRcvData *walrcv = WalRcv;
		SpinLockAcquire(&walrcv->mutex);
		walconn = walrcv->pid != 0;
		SpinLockRelease(&walrcv->mutex);

	}

	lsn = get_current_lsn();

	values[0] = char_to_text_datum(role);
	values[1] = CStringGetTextDatum(sync);
	values[2] = UInt64GetDatum(lsn);
	values[3] = BoolGetDatum(walconn);
	tuple = heap_form_tuple(desc, values, isNulls);
	resultDatum = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(resultDatum);
}

static
void
set_string_guc(const char *name, const char *value)
{
	A_Const aconst = {.type = T_A_Const, .val = {.type = T_String, .val.str = pstrdup(value)}};
	List *args = list_make1(&aconst);
	VariableSetStmt setstmt = {.type = T_VariableSetStmt, .kind = VAR_SET_VALUE, .name = pstrdup(name), .args = args};
	AlterSystemStmt alterSystemStmt = {.type = T_AlterSystemStmt, .setstmt = &setstmt};

	AlterSystemSetConfigFile(&alterSystemStmt);
}

static bool
handle_syncrep()
{
  if (!WalSndCtl->sync_standbys_defined)
  {
    set_string_guc("synchronous_standby_names", "*");
    /* Signal a reload to the postmaster. */
    elog(LOG, "signaling configuration reload: setting synchronous_standby_names to '*'");
    DirectFunctionCall1(pg_reload_conf, PointerGetDatum(NULL) /* unused */);
    // todo: create replication slot
  }
  return 0;
}
static bool
handle_unsyncrep()
{
  if (WalSndCtl->sync_standbys_defined)
  {
    set_string_guc("synchronous_standby_names", "");
    /* Signal a reload to the postmaster. */
    elog(LOG, "signaling configuration reload: setting synchronous_standby_names to ''");
    DirectFunctionCall1(pg_reload_conf, PointerGetDatum(NULL) /* unused */);
    // todo: drop replication slot
  }
  return 0;
}

// todo: log the possible error
static void
SignalPromote(void)
{
  FILE *fd;
  if ((fd = fopen(PROMOTE_SIGNAL_FILE, "w")))
  {
    fclose(fd);
    kill(PostmasterPid, SIGUSR1);
  }
}
static bool
handle_promote()
{
  ControlFileData *control_file;
  DBState state;
  bool ok;
  // promote to primary
  ereport(LOG,
          (errmsg("promoting mirror to primary due to FTS request")));

  /*
   * FTS sends promote message to a mirror.  The mirror may be undergoing
   * promotion.  Promote messages should therefore be handled in an
   * idempotent way.
   */
  control_file = get_controlfile(".", &ok);
  if (!ok)
    elog(ERROR, "incorrect crc for control file");

  state = control_file->state;
  if (state == DB_IN_ARCHIVE_RECOVERY)
  {
    /*
     * Reset sync_standby_names on promotion. This is to avoid commits
     * hanging/waiting for replication till next FTS probe. Next FTS probe
     * will detect this node to be not in sync and reset the same which
     * can take a min. Since we know on mirror promotion its marked as not
     * in sync in gp_segment_configuration, best to right away clean the
     * sync_standby_names.
     */
    handle_unsyncrep();

    // todo: create replication slot
//    CreateReplicationSlotOnPromote(INTERNAL_WAL_REPLICATION_SLOT_NAME);

    SignalPromote();
  }
  else
  {
    elog(LOG, "ignoring promote request, walreceiver not running,"
              " DBState = %d", state);
  }
  // notifies all segments that "I am the new primary."
  return 0;
}

static bool
handle_demote()
{
  return 0;
}
// (action_code, ) => OK
// action:
// 	promote
// 	syncrep
// 	unsyncrep
// 	[demote]
Datum
execute_action(PG_FUNCTION_ARGS)
{
	text *arg0 = PG_GETARG_TEXT_P(0);
	char *action_code = text_to_cstring(arg0);

	if (strcmp(action_code, "promote") == 0)
	{
		handle_promote();
	}
	else if (strcmp(action_code, "syncrep") == 0)
	{
	  handle_syncrep();
	}
	else if (strcmp(action_code, "unsyncrep") == 0)
	{
	  handle_unsyncrep();
	}
	else
	{
		ereport(ERROR, (errmsg("unknown action code '%s'", action_code)));
	}

	PG_RETURN_VOID();
}
