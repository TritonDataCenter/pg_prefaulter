#!/usr/sbin/dtrace -s

#pragma D option quiet
#pragma D option agghist
#pragma D option aggsortkey

BEGIN
{
	start = timestamp
}

zfs_read:entry
/execname == "postgres" && pid == 30330/
{
	self->ts = timestamp;
}

zfs_read:return
/self->ts && timestamp - self->ts > 1000000/
{
	@ = sum(timestamp - self->ts);
	self->ts = 0;
}

zfs_read:return
{
	self->ts = 0;
}

tick-1sec
{
	printf("%d ", timestamp - start);
	printa("%@d", @);
	printf("\n");
}
