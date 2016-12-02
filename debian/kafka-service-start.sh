#!/bin/bash
# vim: set ts=4 sw=4 et:

DAEMON="statbox-kafka"
DAEMON_BASE="statbox"
DAEMON_BIN="/usr/bin/java"
DAEMON_NAME="statbox-kafka"

JAR_COPY="/var/tmp/kafka"
rm -rf $JAR_COPY
mkdir -p $JAR_COPY

CLASSPATH=""
for file in /usr/lib/kafka/libs/*.jar;
do
    cp -f $file $JAR_COPY
    file="$JAR_COPY/`basename $file`"
    CLASSPATH=$CLASSPATH:$file
done
KAFKA_OPTS="-Xmx6144M -server  -Dlog4j.configuration=file:/etc/yandex/kafka/log4j.properties"
KAFKA_CONF="/etc/yandex/kafka/server.properties"

export LC_ALL=C
export LANG=C

if [ -f /etc/default/statbox-kafka ]
then
  . /etc/default/statbox-kafka
fi

CHECKPOINTS="cleaner-offset-checkpoint recovery-point-offset-checkpoint replication-offset-checkpoint"

for dir in `cat $KAFKA_CONF | grep log.dir | cut -d = -f 2 | tr "," " "` 
do 
    echo "check checkpoints in $dir"
    for checkpoint in $CHECKPOINTS
    do
        echo "check $dir/$checkpoint"
        if [ -f $dir/$checkpoint ]
        then
            rm -f $dir/$checkpoint.*
            true 
        else
            latest=`ls -1 $dir/$checkpoint.* 2>/dev/null | tail -1`
            if [ -n $latest ]
            then
                if [ "r$lastest" != "r" ]
                then
                    echo "cp $latest $dir/$checkpoint"
                    cp $latest $dir/$checkpoint
                    chown statbox:statbox $dir/$checkpoint
                    if [ $? = 0 ]
                    then
                        rm -f $dir/$checkpoint.*
                    fi
                fi
            fi
        fi
    done
done

DAEMON_OPTS="$KAFKA_OPTS -cp $CLASSPATH kafka.Kafka $KAFKA_CONF"

ulimit -c unlimited
ulimit -n 655350

test -d /var/run/$DAEMON_BASE || mkdir -p /var/run/$DAEMON_BASE
chown statbox:statbox /var/run/statbox -R
echo "${DAEMON_BIN} -- ${DAEMON_OPTS}"
LD_PRELOAD=$LD_PRELOAD exec start-stop-daemon -c statbox:statbox --start -mp /var/run/$DAEMON_BASE/$DAEMON.pid --exec ${DAEMON_BIN} -- ${DAEMON_OPTS}
