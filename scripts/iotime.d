#!/usr/sbin/dtrace -s

#pragma D option quiet

zfs_read:entry
/execname == "postgres" || execname == "pg_prefaulter"/
{
        self->ts = timestamp;
}

zfs_read:return
/self->ts/
{
        @[execname, pid] = quantize(timestamp - self->ts);
        self->ts = 0;
}

tick-1sec
{
        printa(@);
}
