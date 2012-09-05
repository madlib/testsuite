#!/bin/sh
filename=$1
sqlname=$2
tablename=$3
descname=$4


finalstmt=""
n=0;
while read line; do
   arr=(`echo $line | tr "," "\n"`)
   n=${#arr[@]}
   break
done < $filename
#echo "number of column: $n \n\n"
createstmt="CREATE TABLE $tablename(id SERIAL "

adstmt=""
#echo $descname
if [ $descname != "" ]
then
    while read line; do
	arr=(`echo $line | tr "," "\n"`)
	#echo ${arr[0]}
        loadstmt="$loadstmt ${arr[0]}, "
        createstmt="$createstmt, $line"
    done < $descname
    createstmt="$createstmt )"
    loadstmt=`echo $loadstmt | sed 's/.\{1\}$//'`
else
    for ((  i = 1 ;  i < $n;  i++  ))
    do
        createstmt="$createstmt , V$i text"
        loadstmt="$loadstmt V$i, "
    done

    createstmt="$createstmt, V$n text)"
    
    loadstmt="$loadstmt V$n"
fi
#echo $loadstmt
dropstmt="SET client_min_messages TO WARNING;DROP TABLE IF EXISTS $tablename"
finalstmt="$finalstmt $dropstmt;"

finalstmt="$finalstmt $createstmt ;"

if [[ $filename == *madelon* ]]
then
    finalstmt="$finalstmt COPY $tablename ($loadstmt) FROM stdin DELIMITER ' ' NULL '?' ;"
else
    finalstmt="$finalstmt COPY $tablename ($loadstmt) FROM stdin DELIMITER ',' NULL '?' ;"
fi

echo $finalstmt > $sqlname

if [[ $tablename == *donation ]]
then
    unzip -n $filename
    unzip -n block_1.zip; unzip -n block_2.zip;unzip -n block_3.zip;unzip -n block_4.zip;unzip -n block_5.zip;
    unzip -n block_6.zip; unzip -n block_7.zip;unzip -n block_8.zip;unzip -n block_9.zip
    sed '1d' block_1.csv >> $sqlname
    sed '1d' block_2.csv >> $sqlname
    sed '1d' block_3.csv >> $sqlname
    sed '1d' block_4.csv >> $sqlname
    sed '1d' block_5.csv >> $sqlname
    sed '1d' block_6.csv >> $sqlname
    sed '1d' block_7.csv >> $sqlname
    sed '1d' block_8.csv >> $sqlname
    sed '1d' block_9.csv >> $sqlname
    rm block*
    rm documentation  frequencies.csv
elif [[ $tablename == *donation_test* ]]
then
    unzip -n $filename
    unzip -n block_10.zip
    sed '1d' block_10.csv >> $sqlname
    rm block* documentation  frequencies.csv
else
    cat $filename >> $sqlname

    if [[ $filename == *'adult'* ]]
    then
        sed '$d' $sqlname > tmp; mv tmp $sqlname
    fi

    if [[ $filename == *adult* ]] && [[ $filename == *'test'* ]] 
    then
        sed '2d;s/K\./K/g' $sqlname > tmp; mv tmp $sqlname
    fi
    if [[ $tablename == *nursery* ]]
    then
        sed '/more/d;/^$/d' $sqlname > tmp; mv tmp $sqlname
    fi
    if [[ $tablename == *hypo* ]]
    then
        sed 's/|.*$//g' $sqlname > tmp; mv tmp $sqlname
    fi
    if [[ $tablename == *internet* ]]
    then
        sed 's/\ *?/null/g' $sqlname > tmp; mv tmp $sqlname
    fi

fi
echo "\." >> $sqlname
echo "ALTER TABLE $tablename OWNER TO madlibtester;" >> $sqlname
