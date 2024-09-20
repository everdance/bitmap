#include <postgres.h>

#include "bitmap.h"

IndexBulkDeleteResult *
bmbulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback,
			 void *callback_state)
{
	return NULL;
}


IndexBulkDeleteResult *
bmvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return NULL;
}
