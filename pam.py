# pam.py
# PAM implemenation for pyCluster
# Aug 17, 2013
# Added timing
# May 27, 2013
# daveti@cs.uoregon.edu
# http://daveti.blog.com

from util import importData, pearson_distance, euclidean_distance, manhattan_distance
import random
import sys
import time

# Global variables
initMedoidsFixed = False # Fix the init value of medoids for performance comparison
debugEnabled = True # Debug
distances_cache = {}	# Save the tmp distance for acceleration (idxMedoid, idxData) -> distance
 
def totalCost(data, costF_idx, medoids_idx, cacheOn=False):
	'''
	Compute the total cost and do the clustering based on certain cost function
	'''
	# Init the cluster
	size = len(data)
	total_cost = 0.0
	medoids = {}
	for idx in medoids_idx:
		medoids[idx] = []

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
		total_cost += min_cost

	# Return the total cost and clustering
	return(total_cost, medoids)
     
 
def kmedoids(data, k):
	'''
	kMedoids - PAM implemenation
	See more : http://en.wikipedia.org/wiki/K-medoids
	The most common realisation of k-medoid clustering is the Partitioning Around Medoids (PAM) algorithm and is as follows:[2]
	1. Initialize: randomly select k of the n data points as the medoids
	2. Associate each data point to the closest medoid. ("closest" here is defined using any valid distance metric, most commonly Euclidean distance, Manhattan distance or Minkowski distance)
	3. For each medoid m
		For each non-medoid data point o
			Swap m and o and compute the total cost of the configuration
	4. Select the configuration with the lowest cost.
	5. repeat steps 2 to 4 until there is no change in the medoid.
	'''
	size = len(data)
	medoids_idx = []
	if initMedoidsFixed == False:
		medoids_idx = random.sample([i for i in range(size)], k)
	else:
		medoids_idx = [i for i in range(k)]
	pre_cost, medoids = totalCost(data, 0, medoids_idx)
	if debugEnabled == True:
		print('pre_cost: ', pre_cost)
		print('medioids: ', medoids)
	# Init the results with the current setting
	current_cost = pre_cost
	best_choice = medoids_idx
	best_res = dict(medoids)
	iter_count = 0

	while True:
		for m in medoids:
			for item in medoids[m]:
				# NOTE: both m and item are idx!
				if item != m:
					# Swap m and o - save the idx
					idx = medoids_idx.index(m)
					# This is m actually...
                    			swap_temp = medoids_idx[idx]
                    			medoids_idx[idx] = item
                    			tmp_cost, tmp_medoids = totalCost(data, 0, medoids_idx)
					# Find the lowest cost
                    			if tmp_cost < current_cost:
						best_choice = list(medoids_idx) # Make a copy
                        			best_res = dict(tmp_medoids) 	# Make a copy
                        			current_cost = tmp_cost
					# Re-swap the m and o
					medoids_idx[idx] = swap_temp
		# Increment the counter
		iter_count += 1
		if debugEnabled == True:
			print('current_cost: ', current_cost)
			print('iter_count: ', iter_count)

		if best_choice == medoids_idx:
			# Done the clustering
			break

		# Update the cost and medoids
		if current_cost < pre_cost:
			pre_cost = current_cost
			medoids = best_res
			medoids_idx = best_choice

	return(current_cost, best_choice, best_res)
 

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
		number = len(data)
		if number > 10:
			number = 10
		for i in range(number):
			print('data=', data[i])

	# Add timing here
	startTime = time.time()
	best_cost, best_choice, best_medoids = kmedoids(data, k)
	endTime = time.time()

	print('best_time: ', endTime - startTime)
	print('best_cost: ', best_cost)
	print('best_choice: ', best_choice)
	print('best_medoids: ', best_medoids)


if __name__ == '__main__':
	main()
