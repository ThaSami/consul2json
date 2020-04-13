import consulate
import toolz
import json
from collections import defaultdict

session = consulate.Consul()    

def nested_dict():
    return defaultdict(nested_dict)

nd = nested_dict()
for key, value in session.kv.iteritems():
    nd.update(toolz.assoc_in(nd, key.split('/'), value)) 

with open('test.json','w') as f:
    json.dump(dict(nd),f)