import consulate
import toolz
import json
from collections import defaultdict
import argparse
import sys
import logging
import traceback

def nested_dict():
    return defaultdict(nested_dict)

def checkIfKey(path,session):
    try:
        check = session.kv[path]
    except:
        check = 'notDefined'
            
    if check == 'notDefined':
        return False
    return True

parser = argparse.ArgumentParser()
parser.add_argument('-k','--key', dest='KEY',
                        help="specifies key or path",required=True)
parser.add_argument('-t','--token', dest='TOKEN',
                    help="specify consul token",required=False)
parser.add_argument('--host',dest='HOST', default='localhost', help="defines the host link of consul")
parser.add_argument('-p','--port',dest='PORT', default='8500', help="defines the host port of consul")
parser.add_argument('-s','--scheme',dest='SCHEME', default='http', help="defines connection scheme")

args = parser.parse_args()

session = consulate.Consul(host=args.HOST, port=args.PORT,
                           scheme=args.SCHEME, token=args.TOKEN)    



if checkIfKey(args.KEY,session):
    with open('test.json','w') as f:
        json.dump(session.kv[args.KEY],f)
    sys.exit(0)

try:
    nd = nested_dict()
    for key, value in session.kv.find(args.KEY).items():
        nd.update(toolz.assoc_in(nd, key.split('/'), value)) 

    with open('test.json','w') as f:
        json.dump(dict(nd),f)
        
except Exception as e:
        logging.error(traceback.format_exc())