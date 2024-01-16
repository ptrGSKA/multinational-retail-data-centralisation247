import time
from typing import Any


class DecoratorClass(object):
    '''
    The class is a decorator class that performs the alteration of a function.
    
    Args:
        object: the object, the decorator called on

    Methods:
        __init__(): class constructor
        __get__(): checks the instance and owener of the call
        __call__(): the function is execited when the class is called
        
    '''

    # Class constructor
    def __init__(self, function) -> None:
        '''
        The function is the class constructor.

        Args:
            function: The function the decorator operates, it is automatically passed as the first argument to the init constructor

        Returns:
            None.
        '''
        
        self.function = function

    def __get__(self, instance, owner):
        '''
        The function is to check the instance's owner when the DecoratorClass is called.

        Args:
            instance: the instance ...
            owner: the owner ...

        Returns:
            type of self and the instance and owner of the class call.
        '''
        return type(self)(self.function.__get__(instance, owner))


    def __call__(self, *args, **kwargs) -> Any:
        '''
        If there are decorator arguments, __call__() is only called once as part of the decoration process.
        It performs the function decoration

        Returns:
            The decorated function.
        '''

        ordinal_num = self.function.__name__.split('_')[1]

        print(f'----##- START OF {ordinal_num.upper()} DATA EXTRACTION -##----\n')
        t1 = time.time()

        self.function()

        t2 = time.time()
        print('\n')
        print(f'Time taken to extract, clean and upload to local database is: {round(t2-t1,2)}s')
        print(f'----##- END OF {ordinal_num.upper()} DATA EXTRACTION -##----\n')
        
