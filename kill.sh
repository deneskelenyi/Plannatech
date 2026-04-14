ps auxwww|grep plannatech|grep stream_client|grep -v grep |awk '{print $2}' |xargs kill

