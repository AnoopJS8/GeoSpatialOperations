#! /bin/bash

export PATH=$PATH:/home/vageeshb/spark-1.2.0/bin/:/home/vageeshb/hadoop-2.6.0/bin/
#union
echo "test union"
rm studentOutput
hadoop fs -rm -r /studentOutput

spark-submit --class edu.asu.cse512.Union --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar union-0.1.jar hdfs://master:54310/UnionQueryTestData.csv hdfs://master:54310/studentOutput

hadoop fs -getmerge /studentOutput studentOutput

cp studentOutput unionoutput.txt

if [ $? -eq 0 ]
then 
		echo "union" >> diff.txt
        diff -w studentOutput UnionQueryResult.csv >> diff.txt
        if [ $? -eq 0 ] 
        then
                echo "union success" >> grading.txt
        else
                echo "union failed" >> grading.txt
        fi
else
        echo "union falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput
#FarthestPair
echo "test farthest pair"
spark-submit --class edu.asu.cse512.FarthestPair --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar farthestPair-0.1.jar hdfs://master:54310/FarthestPairTestData.csv hdfs://master:54310/studentOutput

hadoop fs -getmerge /studentOutput studentOutput

cp studentOutput farthestoutput.txt
if [ $? -eq 0 ]
then 
	echo "farthest pair" >> diff.txt
	diff -w studentOutput FarthestPairResult.csv >> diff.txt
	if [ $? -eq 0 ]
	then
		echo "FarthestPair success" >> grading.txt
	else
		echo "FarthestPair failed" >> grading.txt
	fi
else
	echo "FarthestPair falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput
#ClosestPair
echo "test closest pair"
spark-submit --class edu.asu.cse512.ClosestPair --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar closestPair-0.1.jar hdfs://master:54310/ClosestPairTestData.csv hdfs://master:54310/studentOutput

hadoop fs -getmerge /studentOutput studentOutput

cp studentOutput closestpairoutput.txt
if [ $? -eq 0 ]
then 
	echo "closest pair" >> diff.txt
	diff -w studentOutput ClosestPairResult.csv >> diff.txt
	if [ $? -eq 0 ]
	then
		echo "ClosestPair success" >> grading.txt
	else
		echo "ClosestPair failed" >> grading.txt
	fi
else
	echo "ClosestPair falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput
#ConvexHull
echo "test convexhull"
spark-submit --class edu.asu.cse512.convexHull --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar convexHull-0.1.jar hdfs://master:54310/ConvexHullTestData.csv hdfs://master:54310/studentOutput

hadoop fs -getmerge /studentOutput studentOutput

cp studentOutput convexhull.txt
if [ $? -eq 0 ]
then 
	echo "convexhull" >> diff.txt
	diff -w studentOutput ConvexHullResult.csv >> diff.txt
	if [ $? -eq 0 ]
	then
		echo "convexHull success" >> grading.txt
	else
		echo "convexHull failed" >> grading.txt
	fi
else
	echo "convexHull falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput

#Join rectangle
echo "test join rectangle"
spark-submit --class edu.asu.cse512.Join --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar joinQuery-0.1.jar hdfs://master:54310/JoinQueryInput1.csv hdfs://master:54310/JoinQueryInput2.csv hdfs://master:54310/studentOutput rectangle


hadoop fs -getmerge /studentOutput studentOutput
cp studentOutput joinrectangle.txt

if [ $? -eq 0 ]
then 
	echo "join rectangle" >> diff.txt
	diff -w studentOutput JoinQueryRestangleResult.csv >> diff.txt # typo to fix next semester
	if [ $? -eq 0 ]
	then
		echo "join rectangle success" >> grading.txt
	else
		echo "join rectangle failed" >> grading.txt
	fi
else
	echo "join rectangle falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput

#Join point
echo "test join point"
spark-submit --class edu.asu.cse512.Join --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar joinQuery-0.1.jar hdfs://master:54310/JoinQueryInput3.csv hdfs://master:54310/JoinQueryInput2.csv hdfs://master:54310/studentOutput point


hadoop fs -getmerge /studentOutput studentOutput
cp studentOutput joinpoint.txt

if [ $? -eq 0 ]
then 
	echo "join point" >> diff.txt
	diff -w studentOutput JoinQueryPointResult.csv  >> diff.txt
	if [ $?  -eq 0 ]
	then
		echo "join point success" >> grading.txt
	else
		echo "join point failed" >> grading.txt
	fi
else
	echo "join point falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput

#RangeQuery
echo "test range query"
spark-submit --class edu.asu.cse512.RangeQuery --master spark://192.168.42.201:7077 --jars jts.jar,convexHull-0.1.jar rangeQuery-0.1.jar hdfs://master:54310/RangeQueryTestData.csv hdfs://master:54310/RangeQueryRectangle.csv hdfs://master:54310/studentOutput

hadoop fs -getmerge /studentOutput studentOutput

cp studentOutput rangeoutput.txt
if [ $?  -eq 0 ]
then 
	echo "range query" >> diff.txt
	diff -w studentOutput RangeQueryResult.csv
	if [ $? -eq 0 ]
	then
		echo "range query success" >> grading.txt
	else
		echo "range query failed" >> grading.txt
	fi
else
	echo "range query falied" >> grading.txt
fi
rm studentOutput
hadoop fs -rm -r /studentOutput
