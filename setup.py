"""
Build the PyPi package.
"""

import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='gsheets-db-connector',
    version='0.4.0',
    author='Shwetabh Kumar',
    author_email='shwetabh.kumar@fyle.in',
    description='Connects Google sheets to a database connector to transfer information to and fro.',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['fyle', 'db', 'python', 'sdk', 'sqlite'],
    url='https://github.com/fylein/gsheets-db-connector',
    packages=setuptools.find_packages(),
    install_requires=[
        'logger==1.4',
        'pandas==0.25.2',
        'typing==3.7.4.1',
        'gspread==3.1.0',
        'google-api-python-client==1.6.7'
    ],
    classifiers=[
        'Topic :: Internet :: WWW/HTTP',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)
