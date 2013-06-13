# clara.py
# CLARA implemenation for pyCluster
# May 27, 2013
# daveti@cs.uoregon.edu
# http://daveti.blog.com

from util import importData, euclidean_distance, manhattan_distance, pearson_distance
from pam import kmedoids
import random
import sys

# Global variables
debugEnabled = False 	# Debug
claraLoopNum = 5
distances_cache = {}	# Save the tmp distance for acceleration (idxMedoid, idxData) -> distance

def averageCost(data, costF_idx, medoids_idx, cacheOn=False):
	'''
	Compute the average cost of medoids based on certain cost function and do the clustering
	'''
	# Init the cluster
	size = len(data)
	total_cost = {}
	medoids = {}
	for idx in medoids_idx:
		medoids[idx] = []
		total_cost[idx] = 0.0

	# Compute the distance and do the clustering
	for i in range(size):
		choice = -1
		# Make a big number
		min_cost = float('inf')
		for m in medoids:
			if cacheOn == True:
				# Check for cache
				tmp = distances_cache.get((m,i), None)
			if cacheOn == False or tmp == None:
				if costF_idx == 0:
					# euclidean_distance
					tmp = euclidean_distance(data[m], data[i])
				elif costF_idx == 1:
					# manhattan_distance
					tmp = manhattan_distance(data[m], data[i])
				elif costF_idx == 2:
					# pearson_distance
					tmp = pearson_distance(data[m], data[i])
				else:
					print('Error: unknown cost function idx: ' % (costF_idx))
			if cacheOn == True:
				# Save the distance for acceleration
				distances_cache[(m,i)] = tmp
			# Clustering
			if tmp < min_cost:
				choice = m
				min_cost = tmp
		# Done the clustering
		medoids[choice].append(i)
		total_cost[choice] += min_cost

	# Compute the average cost
	avg_cost = 0.0
	for idx in medoids_idx:
		avg_cost += total_cost[idx] / len(medoids[idx])

	# Return the average cost and clustering
	return(avg_cost, medoids)

		
 
def clara(data, k):
	'''
	CLARA implemenation
	1. For i = 1 to 5, repeat the following steps:
	2. Draw a sample of 40 + 2k objects randomly from the
		entire data set,2 and call Algorithm PAM to find
		k medoids of the sample.
	3. For each object Oj in the entire data set, determine
		which of the k medoids is the most similar to Oj.
	4. Calculate the average dissimilarity of the clustering
		obtained in the previous step. If this value is less
		than the current minimum, use this value as the
		current minimum, and retain the k medoids found in
		Step 2 as the best set of medoids obtained so far.
	5. Return to Step 1 to start the next iteration.
	'''
	size = len(data)
	min_avg_cost = float('inf')
	best_choice = []
        best_res = {}

	for i in range(claraLoopNum):
		# Construct the sampling subset
		sampling_idx = random.sample([i for i in range(size)], (40+k*2))
		sampling_data = []
		for idx in sampling_idx:
			sampling_data.append(data[idx])

		# Run kmedoids for the sampling
		pre_cost, pre_choice, pre_medoids = kmedoids(sampling_data, k)
		if debugEnabled == True:
			print('pre_cost: ', pre_cost)
			print('pre_choice: ', pre_choice)
			print('pre_medioids: ', pre_medoids)

		# Convert the pre_choice from sampling_data to the whole data
		pre_choice2 = []
		for idx in pre_choice:
			idx2 = data.index(sampling_data[idx])
			pre_choice2.append(idx2)
		if debugEnabled == True:
			print('pre_choice2: ', pre_choice2)

		# Clustering for all data set
		tmp_avg_cost, tmp_medoids = averageCost(data, 0, pre_choice2)
		if debugEnabled == True:
			print('tmp_avg_cost: ', tmp_avg_cost)
			print('tmp_medoids: ', tmp_medoids)

		# Update the best
		if tmp_avg_cost <= min_avg_cost:
			min_avg_cost = tmp_avg_cost
			best_choice = list(pre_choice2)
			best_res = dict(tmp_medoids)
		
	return(min_avg_cost, best_choice, best_res)
 

def main(): 
	'''
	Main function for PAM
	'''
	if len(sys.argv) != 3:
		print('Error: invalid number of parameters')
		return(1)

	# Get the parameters
	filePath = sys.argv[1]
	k = int(sys.argv[2])
	if debugEnabled == True:
		print('filePath: ', filePath)
		print('k: ', k)

	# Run PAM for europe.txt
	data = importData(filePath)
	if debugEnabled == True:
		for i in range(10):
			print('data=', data[i])

	best_cost, best_choice, best_medoids = clara(data, k)
	print('best_cost: ', best_cost)
	print('best_choice: ', best_choice)
	print('best_medoids: ', best_medoids)


if __name__ == '__main__':
	main()
