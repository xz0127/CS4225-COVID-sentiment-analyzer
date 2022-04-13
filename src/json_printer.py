import json
import pprint

path = "./testing.json"

pp = pprint.PrettyPrinter(indent=4)

f = open(path)

for line in f:
    obj = json.loads(line)
    #print(obj["content"])
    pp.pprint(obj)