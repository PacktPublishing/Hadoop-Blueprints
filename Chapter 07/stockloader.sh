cd staging
if [ "$(ls -A .)" ]
then
hadoop fs -put * /user/hive/warehouse/holding
rm *
else
echo "no filename exists"
fi
