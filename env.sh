cp hive/conf/hive-site-template.xml hive/conf/hive-site.xml
while read line; 
do 
    if [[ ! $line =~ ^# ]]; then
      read a b <<< $(echo $line | awk -F"=" '{print $1" "$2}')
      if [[ $a != "" ]];
      then
        echo replacing \${env:$a} with $b
        sed s#"\${env:$a}"#"$b"#g hive/conf/hive-site.xml > hive/conf/.tmp
        cp hive/conf/.tmp hive/conf/hive-site.xml
      fi;
    fi;
done < .env
rm hive/conf/.tmp
