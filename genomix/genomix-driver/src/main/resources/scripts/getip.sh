#!/bin/bash

#get the OS
os_name=`uname -a|awk '{print $1}'`
linux_os='Linux'

if [ $os_name = $linux_os ];
then
    #Get IP Address
    #Prefer Infiniband connection
#    IPADDR=`/sbin/ifconfig ib0 2> /dev/null | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
#    if [ "$IPADDR" = "" ]
#    then
        ipaddr=`/sbin/ifconfig eth0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        if [ "$ipaddr" = "" ]
        then
            ipaddr=`/sbin/ifconfig lo | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi 
#    fi 
else
        ipaddr=`/sbin/ifconfig en1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
	if [ "$ipaddr" = "" ]
        then
                ipaddr=`/sbin/ifconfig lo0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi

fi
echo $ipaddr
