import time
from typing import Any


# Class definition of the DecoratorClass that modifies the function when it's being called.
class DecoratorClass(object):
    '''
    The class is a decorator class that performs the alteration of a function.

    Parameters:

    
    Args:
        

    Methods:
        __init__():
        __get__():
        __call__():
        
    '''

    # Class constructor
    def __init__(self, function) -> None:
        '''
        The function is the class constructor.

        Returns:
            None.
        '''
        
        self.function = function

    def __get__(self, instance, owner):
        '''
        The function is checks the instance owner when the DecoratorClass is called..

        Returns:
            
        '''
        return type(self)(self.function.__get__(instance, owner))


    def __call__(self, *args, **kwargs) -> Any:
        '''
        This function is being called to decorate the function when the DecoratorClass is called.

        Returns:
            None.
        '''

        print(f'----##- START OF --- DATA EXTRACTION -##----\n')
        t1 = time.time()

        self.function()

        t2 = time.time()
        print('\n')
        print(f'Time taken to extract, clean and upload to local database is: {round(t2-t1,2)}s')
        print(f'----##- END OF --- DATA EXTRACTION -##----\n')
        
