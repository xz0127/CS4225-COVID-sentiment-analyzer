import json
import pprint

path = "./SG_2020_02_01.json"

pp = pprint.PrettyPrinter(indent=4)

f = open(path)

for line in f:
    obj = json.loads(line)
    #print(obj["content"])
    pp.pprint(obj)