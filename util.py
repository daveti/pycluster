# util.py
# Utils for pyCluster
# May 27, 2013
# daveti@cs.uoregon.edu
# http://daveti.blog.com

from math import sqrt
 
def importData(filePath='europe.txt'):
	data = []
	try:
		fnObj = open(filePath, 'r')
		for line in fnObj:
			line = line.strip().split()
			point = []
			for c in line:
				point.append(float(c))
			data.append(tuple(point))
	finally:
		fnObj.close()
	return(data)

def euclidean_distance(vector1, vector2):
	dist = 0
	for i in range(len(vector1)):
		dist += (vector1[i] - vector2[i])**2
	return(dist)

def manhattan_distance(vector1, vector2):
	dist = 0
	for i in range(len(vector1)):
		dist += abs(vector1[i] - vector2[i])
	return(dist)
 
def pearson_distance(vector1, vector2):
	"""
	Calculate distance between two vectors using pearson method
	See more : http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
	"""
	sum1 = sum(vector1)
	sum2 = sum(vector2)

	sum1Sq = sum([pow(v,2) for v in vector1])
	sum2Sq = sum([pow(v,2) for v in vector2])
 
	pSum = sum([vector1[i] * vector2[i] for i in range(len(vector1))])
 
	num = pSum - (sum1*sum2/len(vector1))
	den = sqrt((sum1Sq - pow(sum1,2)/len(vector1)) * (sum2Sq - pow(sum2,2)/len(vector1)))
 
	if den == 0 : return 0.0
	return(1.0 - num/den)
