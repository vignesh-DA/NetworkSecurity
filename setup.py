'''
The setup.py file is an essential part of packaging and distrbutive python projects.
It is used by setup tools (or distutils in older python versions) to define the configuration
of your project, such as its metadata, dependencies, and more  
'''
from setuptools import setup,find_packages
from typing import List

requirement_lst:List[str]=[]

def get_requirements()->List[str]:
    '''
    This function returns the requirements
    '''
    try:
        with open('requirements.txt','r') as file:
            # Read lines from the files
            lines=file.readlines()
            # process each line
            for line in lines:
                requirement=line.strip()
                # ignore empty lines and -e .
                if requirement and requirement!='-e .':
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found")
    return requirement_lst

setup(
    name='Network Security',
    version="0.0.1",
    author='Gogula Vignesh',
    packages=find_packages(),
    install_requires=get_requirements()
)