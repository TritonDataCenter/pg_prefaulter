#!/usr/sbin/dtrace -s

#pragma D option quiet
#pragma D option switchrate=50hz
#pragma D option cleanrate=500hz
#pragma D option dynvarsize=16m

zfs_read:entry
/execname == "postgres" || execname == "pg_prefaulter"/
{
        self->ts = timestamp;
        self->vp = args[0];
        self->offs = args[1]->_uio_offset._f;
        self->size = args[1]->uio_resid;
}

zfs_read:return
/self->ts/
{
        printf("%d %d %s/%d/%d %s offs=%d size=%d\n", timestamp,
            timestamp - self->ts,
            execname, pid, tid,
            basename(stringof(self->vp->v_path)),
            self->offs, self->size);
        self->ts = 0;
        self->vp = NULL;
        self->offs = 0;
        self->size = 0;
}

tick-1sec
/i++ > 600/
{
        exit(0);
}
