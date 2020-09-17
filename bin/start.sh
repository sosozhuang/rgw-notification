#!/bin/bash

case "`uname`" in
    Linux)
    bin_abs_path=$(readlink -f $(dirname $0))
    ;;
  *)
    bin_abs_path=`cd $(dirname $0); pwd`
    ;;
esac
bin=`dirname "$0"`
base=`cd "$bin"/..; pwd`
notification_config="$base/config/notification.properties"
logging_config="$base/config/log4j2.xml"
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/notification.pid ] ; then
  echo "Notification service is running, should stop first." 2>&2
  exit 1
fi

if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

if [ -z "$JAVA" ]; then
  JAVA=$(readlink -f /usr/bin/java | sed "s:bin/java::")/bin/java
fi
if [ -z "$JAVA" ]; then
  echo "Failed to locate java executable." 2>&2
  exit 1
fi

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
    JAVA_OPTS="-server -Xms1g -Xmx1g -Xss1024k -XX:+AlwaysPreTouch -XX:MaxGCPauseMillis=100 -XX:+UseG1GC -XX:MaxTenuringThreshold=6 -XX:InitiatingHeapOccupancyPercent=75 -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=50 -XX:+ParallelRefProcEnabled"
else
    JAVA_OPTS="-server -Xms512m -Xmx512m -XX:MaxGCPauseMillis=200 -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=75"
fi

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
PRG_OPTS="-Dlog4j2.properties=$logging_config -Dnotification.config.location=$notification_config"

if [ -e $notification_config -a -e $logging_config ]
then
  for i in $base/lib/*;
    do CLASSPATH=$i:"$CLASSPATH";
    done
    CLASSPATH="$base/config:$CLASSPATH";

  echo logging config location: $logging_config
  echo notification config location: $notification_config
  echo CLASSPATH:$CLASSPATH
  exec nohup $JAVA $JAVA_OPTS $PRG_OPTS -classpath .:$CLASSPATH io.ceph.rgw.notification.NotificationService 1>>nohup.out 2>&1 &
  echo $! > $base/bin/notification.pid
  echo 'Notification service started.'
else
  echo "Notification config("$notification_config") or log4j2 config file($logging_config) not exist."
fi
