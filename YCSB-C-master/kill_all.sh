#!/bin/bash
source setting.sh
ssh -p ${port_m} ${user}@${master} " killall master "
ssh -p ${port_p0} ${user}@${primary0} " killall primary "
ssh -p ${port_p1} ${user}@${primary1} " killall primary "
ssh -p ${port_p2} ${user}@${primary2} " killall primary "
ssh -p ${port_p3} ${user}@${primary3} " killall primary "
ssh -p ${port_b0} ${user}@${backup0} " killall backup "
ssh -p ${port_b1} ${user}@${backup1} " killall backup "
ssh -p ${port_e0} ${user}@${extra0} " killall extra "
ssh -p ${port_e1} ${user}@${extra1} " killall extra "

