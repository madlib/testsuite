#nsf_convert.sh data1 data2 data3 data4 table sql
data1=$1
data2=$2
data3=$3
data4=$4
table=$5
sql=$6

mkdir nsf_abs
unzip $data1 -d nsf_abs
unzip $data2 -d nsf_abs
unzip $data3 -d nsf_abs
unzip $data4 -d nsf_abs

python run.py nsf_abs $table
python sql.py $table $sql
#rm -rf nsf_abs
