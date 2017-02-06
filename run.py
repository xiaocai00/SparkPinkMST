import os
PROJECT_HOME="/root/"
SPARK_JAR="/Users/cjin/git/spark/assembly/target/scala-2.10/spark-assembly-1.0.0-SNAPSHOT-hadoop2.0.0-cdh4.6.0.jar"
PINK_JAR="target/pink-1.0.jar"
# dataSetDict = {
# # "clust32k": 100,
# # "clust128k": 100,
# # "clust512K": 100,
# "clust2M": 100}

dataSetDict = {
"clust32k" : 32 * 1024,
"clust128k": 128 * 1024,
"clust512K": 512 * 1024,
"clust2M"  : 2 * 1024 * 1024}

numDimensionsArr=[5, 10]
numSplitsArr=[2]
Karr=[3]

dataSetDict = {
"clust32k" : 100,
}
numDimensionsArr=[5]

def runPink(dataSetName, numPoints, numDimensions, numSplits, K):
	args=["pink.Pink", dataSetName, numPoints, numDimensions, numSplits, K, "./sparkConfig", PROJECT_HOME]
	argsStr = map(lambda x: str(x), args)
	cmd = "java -cp %s %s >& log_%s_n%d_d%d_s%d_k%d"\
		%(":".join([SPARK_JAR, PINK_JAR]), " ".join(argsStr), dataSetName, 
			numPoints, numDimensions, numSplits, K) 
	print cmd
	os.system(cmd)

if __name__ == "__main__":
	for dataSetName in dataSetDict:
		numPoints = dataSetDict[dataSetName]
		for numDimensions in numDimensionsArr:
			for numSplits in numSplitsArr:
				for K in Karr:
					runPink(dataSetName, numPoints, numDimensions, numSplits, K)
