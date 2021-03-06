import os
import numpy as np


os.system("$HADOOP_HOME/bin/hadoop fs -rm -r data")
os.system("$HADOOP_HOME/bin/hadoop fs -rm -r mystery_data")
os.system("rm -r data")
os.system("rm -r mystery_data")

os.system("$HADOOP_HOME/bin/hadoop jar cs435_pa2.jar offline.MainClass input/gutenberg.txt data/aavs")
os.system("$HADOOP_HOME/bin/hadoop jar cs435_pa2.jar online.MainClass input/xyz2_text.txt mystery_data/")
os.system("$HADOOP_HOME/bin/hadoop fs -get data ./")
os.system("$HADOOP_HOME/bin/hadoop fs -get mystery_data ./")

num_file = open('mystery_data/cos_sim_numerators/part-r-00000','r')
denom_file = open('mystery_data/cos_sim_denominators/part-r-00000','r')
outfile = open('results','w')

cos_nums = []
cos_denoms = []
cos_sims = []

unk_denom = None
for line in num_file:
	split = line.split("\t")
	cos_nums.append([split[0], (float(split[1]))])
for line in denom_file:
	split = line.split("\t")
	if (split[0] == "unknown author"):
		unk_denom = float(split[1])
	else:
		cos_denoms.append([split[0], (float(split[1]))])
	
for auth in cos_nums:
	cos_numer = auth[1]
	for d_auth in cos_denoms:
		if (auth[0] == d_auth[0]):
			cos_denom = d_auth[1] * unk_denom
			sim = cos_numer / cos_denom
			cos_sims.append([sim, auth[0]])

ordered_cs = []
mx = 2.0
curr = 0.0
curr_auth = None
for i in range(len(cos_sims)):
    for e in cos_sims:
        if ((e[0] > curr) & (e[0] < mx)):
            curr = e[0]
            curr_auth = e[1]
    ordered_cs.append([curr, curr_auth])
    mx = curr
    curr = 0.0

cos_sims = ordered_cs

count = 9
if (len(cos_sims) < 10):
	count = len(cos_sims)-1

while(count >= 0):
	name = cos_sims[len(cos_sims) - 1 - count][1]
	val = cos_sims[len(cos_sims) - 1 - count][0]
	#print("{}   author:   {}    cos_sim: {}".format((len(cos_sims) - count), name, val))
	#outfile.write("{}   author:   {}    cos_sim: {}".format((len(cos_sims) - count), name, val))
        print("{}   author:   {}".format((len(cos_sims) - count), name))
        outfile.write("{}   author:    {}".format((len(cos_sims) - count), name))
	count -= 1
