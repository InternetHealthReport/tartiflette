# coding: utf-8
import cPickle as pickle
from routeAnalysis import routeCountRef
from routeAnalysis import ddType
print "loading the pickle"
ref = pickle.load(open("saved_references/583efbc255a0a9b2deadc910_routeChange.pickle"))

print "cleaning the reference"
ktr = []
for k in ref.keys():
    if len(ref[k])<500:
        ktr.append(k)
        

for k in ktr:
    del ref[k]

print "dumping to disk"
pickle.dump(ref,open("saved_references/new_routeRef.pickle","wb"))
print "done! (rename the new pickle if you want to use it)"
