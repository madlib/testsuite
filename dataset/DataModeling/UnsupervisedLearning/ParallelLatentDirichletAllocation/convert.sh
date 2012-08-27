#convert.sh data table sql
data=$1
table=$2
sql=$3

if [[ $data == *'ReutersTranscribedSubset'* ]]
then
    unzip $data
    folder=transcriptions
elif [[ $data == *'reuters21578'* ]]
then
    mkdir reuters21578
    tar -xzvf $data -C reuters21578
    folder=reuters21578
else
    tar -xzvf $data
    folder=$(basename $data | cut -d '.' -f1)
fi

python run.py $folder $table
python sql.py $table $sql

rm -rf $folder
rm $table.dict
rm $table.madlib
