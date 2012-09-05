#./conv_flare.sh <data1> <data2> <desc> <sql> <table>

data1=$1
data2=$2
desc=$3
sql=$4
table=$5

sed '1d' $1 > tmp
sed '1d' $2 >> tmp

./conv_linregr.py -t $5 -o $4 -d tmp -D $3

rm tmp
