import os
import yaml

path =  os.path.realpath(__file__)
dir = os.path.dirname(path)
cred_dir = dir.replace('database','creds')

def cred_extract(document, subkey):
    try:
        with open(os.path.join(cred_dir,'credentials.yaml'), mode = 'r') as stream:
            creds = yaml.safe_load_all(stream)

            while True:
                cred_docs = next(creds)

                for mainkey in cred_docs:
                    if mainkey == document:
                        subkeys = cred_docs[mainkey].keys()
                        for sub in subkeys:
                            if sub == subkey:
                                for dict in cred_docs[mainkey][subkey]:
                                    return dict

    except Exception as e:
        print(e)

ooo = cred_extract('Links','Link')['s3_address']
print(ooo)