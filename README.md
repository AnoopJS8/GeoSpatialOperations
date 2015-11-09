# cse512-15fall-project
Project for CSD512 Distribute and Parallel Database, 15 Fall semester, Arizona State University.

For student, please don't directly for this project.

First check if you have right to create a private repository, if you don't apply for [github edu pack](https://education.github.com/pack).

Create a new repository, and choose import from existing repository. Now you're ready to start.

I strongly every one use different account to commit to your group project. So that you have a commit history for each group members.

## Running the operations ##

* Union
```
./spark-submit \
	--class edu.asu.cse512.Union \
	--master spark://192.168.42.201:7077  \
	--jars /home/vageeshb/.m2/repository/com/vividsolutions/jts/1.13/jts-1.13.jar \
	~/workspace/awesome-hexta-geospatial/fullProjectAssembly/target/union-0.1.jar <inputFilename> <outputFilename>

```

* Closest Pair
```
./spark-submit \
	--class edu.asu.cse512.ClosestPair \
	--master spark://192.168.42.201:7077  \
	--jars ~/workspace/awesome-hexta-geospatial/fullProjectAssembly/target/convexHull-0.1.jar \
	~/workspace/awesome-hexta-geospatial/fullProjectAssembly/target/closetPair-0.1.jar <inputFilename> <outputFilename>

```
