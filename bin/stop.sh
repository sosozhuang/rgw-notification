#!/bin/bash

cygwin=false;
linux=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
    Linux*)
    	linux=true
    	;;
esac

get_pid() {
	P_NAME=$1
	PID=$2
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
	    if [ ! -z "$PID" ]; then
	       JAVA_PID=`ps aux |grep "$P_NAME"|grep "$PID"|grep -v grep|awk '{print $2}'`
		else
		   JAVA_PID=`ps aux |grep "$P_NAME"|grep -v grep|awk '{print $2}'`
	    fi
    fi
    echo $JAVA_PID;
}

bin=`dirname "$0"`
base=`cd "$bin"/..; pwd`
pid_file="$base/bin/notification.pid"
if [ ! -f "$pid_file" ];then
	echo "Notification service is not running."
	exit
fi

pid=`cat $pid_file`
if [ "$pid" == "" ] ; then
	pid=`get_pid "NotificationService"`
fi

echo -e "Stopping Notification service,process id $pid..."
kill $pid

while (true);
do
	gpid=`get_pid "NotificationService" "$pid"`
    if [ "$gpid" == "" ] ; then
    	echo "Notification service stopped."
    	`rm $pid_file`
    	break;
    fi
    sleep 1
done