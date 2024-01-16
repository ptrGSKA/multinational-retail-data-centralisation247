import os
import yaml


class CredentialReader:
    '''
    The class is used to extract credentials safely from a multidocument yaml file.
    It reads the database credentials, API keys, links and endpoints, database tables also.
 
    Methods:
        __init__():
        credential_extraction(): 
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.

        Args:
            path: real path to the source file where it was called
            dir: the directory of the file
            cred_dir: full path pointing to the creds directory in any operating system

        Returns:
            None
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.cred_dir = self.dir.replace('source','creds')

    def credential_extraction(self, document, subkey):
        '''
        This function reads credentials from a multidocument yaml file based on the required document and subkey.

        Args:
            document: the document in the yaml file that contains the necessary information
            subkey: within the document specifies the final destination that contains the information being requested

        Returns:
            Returns a dictionary with the credentials, links, keys or endpoints.
        '''

        try:
            with open(os.path.join(self.cred_dir,'credentials.yaml'), mode = 'r') as stream:
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
