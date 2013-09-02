#!/bin/bash

#get the OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

if [ $OS_NAME = $LINUX_OS ];
then
    #Get IP Address
    #Prefer Infiniband connection
#    IPADDR=`/sbin/ifconfig ib0 2> /dev/null | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
#    if [ "$IPADDR" = "" ]
#    then
        IPADDR=`/sbin/ifconfig eth0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        if [ "$IPADDR" = "" ]
        then
            IPADDR=`/sbin/ifconfig lo | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi 
#    fi 
else
        IPADDR=`/sbin/ifconfig en1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
	if [ "$IPADDR" = "" ]
        then
                IPADDR=`/sbin/ifconfig lo0 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
        fi

fi
echo $IPADDR