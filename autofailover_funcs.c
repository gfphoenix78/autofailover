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


//PG_FUNCTION_INFO_V1(collect_status);
//PG_FUNCTION_INFO_V1(execute_action);
PG_FUNCTION_INFO_V1(autofailover_execute);

static void handle_promote(void);
static void handle_syncrep(void);
static void handle_unsyncrep(void);

extern int max_wal_senders;

static ControlFileData *
get_control_file() {
  static ControlFileData *_ctl_file = NULL;
  if (_ctl_file == NULL) {
    bool found;
    _ctl_file = (ControlFileData *)ShmemInitStruct("Control File",
  					sizeof(ControlFileData), &found);
    Assert(found && "found control file");
  }
  Assert(_ctl_file);
  return _ctl_file;
}

static char probe_role()
{
  int rc;
  rc = access("standby.signal", F_OK | R_OK);
  if (rc == 0)
    return 's';

  if (errno != ENOENT)
    ereport(ERROR, (errmsg("open standby.signal: %m")));

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
static void
validate_last_role(const char *last_role)
{

}
Datum
autofailover_execute(PG_FUNCTION_ARGS)
{
  char role;
  char last_role;
  bool walconn = false;
  const char *sync_state = "?";
  const char *syncrep = "?";
  volatile char **const p_standby_names = &SyncRepStandbyNames;
  XLogRecPtr lsn;

  TupleDesc desc;
  TypeFuncClass funcClass;
  HeapTuple tuple;

  funcClass = get_call_result_type(fcinfo, NULL, &desc);
  if (funcClass != TYPEFUNC_COMPOSITE)
  {
    ereport(ERROR, (errmsg("return type must be a row type")));
  }
  text *arg0 = PG_GETARG_TEXT_P(0);
  char *cmd = text_to_cstring(arg0);

  last_role = '?';
  if (strcmp(cmd, "status") == 0)
  {
    text *arg1 = PG_GETARG_TEXT_P(1);
    char *last_role_ = text_to_cstring(arg1);
    validate_last_role(last_role_);
    last_role = last_role_[0];
  }
  else if (strcmp(cmd, "promote") == 0)
  {
    handle_promote();
  }
  else if (strcmp(cmd, "syncrep") == 0)
  {
    handle_syncrep();
  }
  else if (strcmp(cmd, "unsyncrep") == 0)
  {
    // fixme: wait until the GUC is set in all backends and all next queries returns syncrep=''
    handle_unsyncrep();
//    LWLockAcquire(SyncRepLock, LW_SHARED);
//    while(WalSndCtl->sync_standbys_defined)
//    {
//      LWLockRelease(SyncRepLock);
//      usleep(800 * 1000);
//      LWLockAcquire(SyncRepLock, LW_SHARED);
//    }
//    LWLockRelease(SyncRepLock);
  }
  else if (strcmp(cmd, "info") == 0)
  {
//	  handle_info();
    elog(LOG, "get initial info");
  }
  else
  {
    ereport(ERROR, (errmsg("unknown action code '%s'", cmd)));
  }

  // return current status
  Datum resultDatum;
  Datum values[5];
  bool isNulls[5];

  memset(values, 0, sizeof(values));
  memset(isNulls, 0, sizeof(isNulls));

  role = probe_role();
elog(LOG, "max_wal_senders=%d", max_wal_senders);
  if (role == 'p')
  {
    int i;
    LWLockAcquire(SyncRepLock, LW_SHARED);
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
          sync_state = state == WALSNDSTATE_STREAMING
                 ? "streaming" : "catchup";
          walconn = true;
          break;
        default:
          break;
      }
      if (state == WALSNDSTATE_STREAMING)
        break;
    }
    if (WalSndCtl->sync_standbys_defined)
      syncrep = "*";
    else
      syncrep = "f";
    LWLockRelease(SyncRepLock);
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
  values[1] = CStringGetTextDatum(syncrep);
  values[2] = CStringGetTextDatum(sync_state);
  values[3] = UInt64GetDatum(lsn);
  values[4] = BoolGetDatum(walconn);
  tuple = heap_form_tuple(desc, values, isNulls);
  resultDatum = HeapTupleGetDatum(tuple);
  PG_RETURN_DATUM(resultDatum);
}
//Datum
//collect_status(PG_FUNCTION_ARGS)
//{
//	char role;
//	bool walconn = false;
//	const char *sync = "?";
//	XLogRecPtr lsn = 0;
//
//	TupleDesc desc;
//	TypeFuncClass funcClass;
//	HeapTuple tuple;
//	Datum resultDatum;
//	Datum values[5];
//	bool isNulls[5];
//
//	funcClass = get_call_result_type(fcinfo, NULL, &desc);
//	if (funcClass != TYPEFUNC_COMPOSITE)
//	{
//		ereport(ERROR, (errmsg("return type must be a row type")));
//	}
//
//	memset(values, 0, sizeof(values));
//	memset(isNulls, 0, sizeof(isNulls));
//
//	role = probe_role();
//
//	if (role == 'p')
//	{
//		int i;
//		for (i = 0; i < max_wal_senders; i++)
//		{
//			WalSnd *walsnd = &WalSndCtl->walsnds[i];
//			WalSndState state;
//			if (walsnd->pid == 0)
//				continue;
//			SpinLockAcquire(&walsnd->mutex);
//			state = walsnd->state;
//			SpinLockRelease(&walsnd->mutex);
//			switch (state)
//			{
//				case WALSNDSTATE_STREAMING:
//				case WALSNDSTATE_CATCHUP:
//					sync = state == WALSNDSTATE_STREAMING
//								? "streaming" : "catchup";
//					walconn = true;
//					break;
//				default:
//					break;
//			}
//			if (state == WALSNDSTATE_STREAMING)
//				break;
//		}
//	}
//	else
//	{
//		WalRcvData *walrcv = WalRcv;
//		SpinLockAcquire(&walrcv->mutex);
//		walconn = walrcv->pid != 0;
//		SpinLockRelease(&walrcv->mutex);
//
//	}
//
//	lsn = get_current_lsn();
//	values[0] = char_to_text_datum(role);
//	values[1] = CStringGetDatum(SyncRepStandbyNames);
//  values[2] = CStringGetTextDatum(sync);
//  values[3] = UInt64GetDatum(lsn);
//	values[4] = BoolGetDatum(walconn);
//	tuple = heap_form_tuple(desc, values, isNulls);
//	resultDatum = HeapTupleGetDatum(tuple);
//	PG_RETURN_DATUM(resultDatum);
//}

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

static void
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
}
static void
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

static void
handle_promote()
{
  ControlFileData *control_file;
  DBState state;
  // promote to primary
  ereport(LOG,
          (errmsg("promoting mirror to primary due to FTS request")));

  /*
   * FTS sends promote message to a mirror.  The mirror may be undergoing
   * promotion.  Promote messages should therefore be handled in an
   * idempotent way.
   */
  control_file = get_control_file();

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
    elog(LOG, "ignoring promote request, not in archive recovery,"
              " DBState = %d", state);
  }
  // notifies all segments that "I am the new primary."
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

//Datum
//execute_action(PG_FUNCTION_ARGS)
//{
//	text *arg0 = PG_GETARG_TEXT_P(0);
//	char *action_code = text_to_cstring(arg0);
//
//	if (strcmp(action_code, "promote") == 0)
//	{
//		handle_promote();
//	}
//	else if (strcmp(action_code, "syncrep") == 0)
//	{
//	  handle_syncrep();
//	}
//	else if (strcmp(action_code, "unsyncrep") == 0)
//	{
//	  handle_unsyncrep();
//	}
//	else if (strcmp(action_code, "info") == 0)
//  {
////	  handle_info();
//  }
//	else
//	{
//		ereport(ERROR, (errmsg("unknown action code '%s'", action_code)));
//	}
//
//	PG_RETURN_VOID();
//}
