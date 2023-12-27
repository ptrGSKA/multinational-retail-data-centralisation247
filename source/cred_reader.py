import os
import yaml


# Class definition of the Data Extractor class for extraction of data from multiple sources.
class CredentialReader:
    '''
    The class is used to extract credentials safely from a multidocument yaml file.

    Parameters:
        
    
    Args:
        

    Methods:
        credential_extraction(): 
    '''

    # Class constructor
    def __init__(self) -> None:
        '''
        The constructor initializes the attributes that necessary for the class instances.
        '''
        self.path =  os.path.realpath(__file__)
        self.dir = os.path.dirname(self.path)
        self.cred_dir = self.dir.replace('source','creds')

    def credential_extraction(self,document, subkey):
        '''
        This function reads credentials from a multidocument yaml file based on the required document and subkey.

        Returns:
            Returns a dictionary
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