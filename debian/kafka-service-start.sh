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

DAEMON_OPTS="$KAFKA_OPTS -cp $CLASSPATH kafka.Kafka $KAFKA_CONF"

ulimit -c unlimited
ulimit -n 655350

test -d /var/run/$DAEMON_BASE || mkdir -p /var/run/$DAEMON_BASE
chown statbox:statbox /var/run/statbox -R
echo "${DAEMON_BIN} -- ${DAEMON_OPTS}"
LD_PRELOAD=$LD_PRELOAD exec start-stop-daemon -c statbox:statbox --start -mp /var/run/$DAEMON_BASE/$DAEMON.pid --exec ${DAEMON_BIN} -- ${DAEMON_OPTS}
