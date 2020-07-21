from numpy import genfromtxt
txt = genfromtxt('fileName.csv', delimiter='|', names=True, encoding='utf-8', dtype=None)
txt
