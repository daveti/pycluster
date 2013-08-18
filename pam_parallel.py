# pam_parallel.py
# Parallel PAM implemenation for pyCluster
# Aug 17, 2013
# daveti@cs.uoregon.edu
# http://daveti.blog.com

import util
import random
import sys
import time
import pp

# Global variables
initMedoidsFixed = False # Fix the initial medoids (used to do performance comparison)
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
					tmp = util.euclidean_distance(data[m], data[i])
				elif costF_idx == 1:
					# manhattan_distance
					tmp = util.manhattan_distance(data[m], data[i])
				elif costF_idx == 2:
					# pearson_distance
					tmp = util.pearson_distance(data[m], data[i])
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
     
def kmedoids_task(cluster, m, medoids_idx, current_cost, data):
	'''
	Working task for kmedoids_parallel
	For each non-medoid data point o
		Swap m and o and compute the total cost of the configuration
	Reture the configuration with the lowest cost.
	Note: each task should have its own medoids_idx list.
	'''
	best_choice = []
	best_res = {}

	for item in cluster:
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
				best_res = dict(tmp_medoids)    # Make a copy
				current_cost = tmp_cost
			# Re-swap the m and o
			medoids_idx[idx] = swap_temp

	return(current_cost, best_choice, best_res)
 
def kmedoids_parallel(data, k, t):
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

	kMedoids Parallel - Parallel PAM implemenation
	t - used to determine the number of working tasks
	Actually, there should be a relationship among the factors below:
		1. number of CPUs in the machine - c
		2. number of clusters expected - k
		3. number of working tasks expected - t
	The perfect mapping for these would be k=t=c, I think...
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
		#print('medioids: ', medoids)
	current_cost = pre_cost
	best_choice = []
	best_res = {}
	iter_count = 0

	# Init the parallel python
	jobs = []
	ppservers = ()
	jobServer = pp.Server(t, ppservers=ppservers)
	print('Starting parallel python with %d CPUs and %d working tasks' %(jobServer.get_ncpus(), t))

	while True:
		for m in medoids:
			# Make a copy for medoids_idx for each task
			medoids_idx_task = list(medoids_idx)
			# Submit the job to the server
			job = jobServer.submit(kmedoids_task, (medoids[m], m, medoids_idx_task, current_cost, data), (totalCost,), ("util",))
			# Add the job
			jobs.append(job)

		# Print job statistics
		if debugEnabled == True: 
			jobServer.print_stats()

		# Find the configuration with the lowest cost
		bestConf = jobs[0]()
		for j in jobs:
			if j()[0] < bestConf[0]:
				bestConf = j()
		current_cost = bestConf[0]
		best_choice = bestConf[1]
		best_res = bestConf[2]

		# Increment the counter
		iter_count += 1
		if debugEnabled == True:
			print('current_cost: ', current_cost)
			print('iter_count: ', iter_count)

		if best_choice == medoids_idx:
			# Done the clustering
			break

		# Update the cost and medoids
		if current_cost <= pre_cost:
			pre_cost = current_cost
			medoids = best_res
			medoids_idx = best_choice

	return(current_cost, best_choice, best_res)
 

def main(): 
	'''
	Main function for Parallele PAM
	'''
	if len(sys.argv) != 4:
		print('Error: invalid number of parameters')
		return(1)

	# Get the parameters
	filePath = sys.argv[1]
	k = int(sys.argv[2])
	t = int(sys.argv[3])
	if debugEnabled == True:
		print('filePath: ', filePath)
		print('k: ', k)
		print('t: ', t)

	# Run PAM for europe.txt
	data = util.importData(filePath)
	if debugEnabled == True:
		for i in range(10):
			print('data=', data[i])

	# Check the timing
	startTime = time.time()
	best_cost, best_choice, best_medoids = kmedoids_parallel(data, k, t)
	endTime = time.time()

	print('best_time: ', endTime - startTime)
	print('best_cost: ', best_cost)
	print('best_choice: ', best_choice)
	print('best_medoids: ', best_medoids)


if __name__ == '__main__':
	main()
