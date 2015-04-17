#!/bin/bash

remove="statbox-kafka"

for srv in  $remove
do
    if [ -f /etc/init.d/$srv ]
    then
		mkdir /var/lock/statbox
		touch /var/lock/statbox/statbox-logbroker-watchdog
        /etc/init.d/$srv force-stop
        update-rc.d -f $srv remove
        rm -f /etc/init.d/$srv
        service statbox-kafka start
		rm /var/lock/statbox/statbox-logbroker-watchdog
    fi
done

