import consulate
import toolz
import json
from collections import defaultdict
import argparse
import logging
import traceback

class Consul2Json():
    
    def __init__(self,host,port,scheme,token):
        self.host = host
        self.port = port 
        self.scheme = scheme 
        self.token = token
        self.session = consulate.Consul(host=host, port=port,
                           scheme=scheme, token=token)    

    def nested_dict(self):
        return defaultdict(self.nested_dict)

    def checkIfKey(self,path):
        ''' checks if the given path is a key with a value '''
        try:
            check = self.session.kv[path]
            return True
        except:
            return False

    def getKey(self,key):
        ''' return the associated value given a key '''
        try:
            result = self.session.kv[key]
            return result
        except Exception as e:
            logging.error(traceback.format_exc())
            return "notDefined" 
    

    def getIfPath(self,path):
        ''' returns a structured nested dictionary with all values and keys in a given path ''' 
        try:
            nd = self.nested_dict()
            keylen = len(path.split('/'))
            if path.endswith('/'):
                keylen = len(path.split('/')) - 1
                
            for key, value in self.session.kv.find(path).items():
                keysToPut = [i for i in key.split('/')[keylen:]] 
                nd.update(toolz.assoc_in(nd, keysToPut, value)) 
            return dict(nd)  

        except Exception as e:
            logging.error(traceback.format_exc())
            return {}

    def getVal(self,path):
        ''' try to get a value if it is a key, if it failed it will try to get it as a path. '''
        if self.checkIfKey(path):
            return self.getKey(path)
        return self.getIfPath(path)


    def describe(self):
        return "host {},port {}, scheme {}, token {}".format(self.host,self.port,self.scheme,self.token)
    

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-k','--key', dest='KEY',
                            help="specifies key or path",required=True)  

    parser.add_argument('-t','--token', dest='TOKEN',
                        help="specify consul token",required=False)
    parser.add_argument('--host',dest='HOST', default='localhost', help="defines the host link of consul")
    parser.add_argument('-p','--port',dest='PORT', default='8500', help="defines the host port of consul")
    parser.add_argument('-s','--scheme',dest='SCHEME', default='http', help="defines connection scheme")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    pass
    args = getArgs()
    c2j = Consul2Json(host=args.HOST, port=args.PORT,
                           scheme=args.SCHEME, token=args.TOKEN)    
    print(c2j.getVal(args.KEY))