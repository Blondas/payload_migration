from setuptools import setup, find_packages

setup(
    name='unit_of_work',  # 1. Package name
    version='0.1.0',      # 2. Version
    description='An ETL (Extract, Transform, Load) Python package for payload migration processing, named "Unit of Work." It extracts by waiting for and confirming the existence of an imported tape, transforms by slicing the tape, performing sanity checks, and creating a future directory structure via symbolic links, and loads by uploading the processed data to S3.',
    long_description=open('README.md').read(),  # 4. Full description from README
    long_description_content_type='text/markdown',  # 5. Specify README format
    author='Krzysztof Nowakowski',  # 6. Author
    author_email='knowakowski@pm.me',  # 7. Contact email

    packages=find_packages(),  # 8. Package structure

    install_requires=[         # 9. Dependencies
        'ibm_db==3.2.3',
        'PyYAML==6.0.2'
    ],

    python_requires='>=3.9',  # 10. Required Python version
    classifiers=[             # 11. Classifiers for PyPI
        'Programming Language :: Python :: 3',
        'Operating System :: AIX',
    ],

    entry_points={            # 12. Command-line scripts
        'console_scripts': [
            'unit-of-work = unit_of_work.__main__:main',
        ],
    }
)