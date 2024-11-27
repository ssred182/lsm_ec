#!/bin/bash
sleep 500
ssh -p ${port_p0} ${user}@${primary0} "kill "
#kill pid of the primary process