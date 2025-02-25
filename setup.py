from setuptools import setup, find_packages

setup(
    name='unit_of_work', 
    version='0.1.0',     
    description='An ETL (Extract, Transform, Load) Python package for payload migration processing, named "Unit of Work." It extracts by waiting for and confirming the existence of an imported tape, transforms by slicing the tape, performing sanity checks, and creating a future directory structure via symbolic links, and loads by uploading the processed data to S3.',
    long_description=open('README.md').read(),  
    long_description_content_type='text/markdown', 
    author='Krzysztof Nowakowski', 
    author_email='knowakowski@pm.me',  

    packages=find_packages(), 

    setup_requires=[
        'setuptools==58.1.0'
    ],
    
    install_requires=[        
        'ibm_db==3.2.3',
        'PyYAML==6.0.2'
    ],

    python_requires='>=3.9', 
    classifiers=[            
        'Programming Language :: Python :: 3',
        'Operating System :: AIX',
    ],

    entry_points={           
        'console_scripts': [
            'unit-of-work = unit_of_work.__main__:main',
        ],
    }
)