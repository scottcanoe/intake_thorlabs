# -*- coding: utf-8 -*-

from setuptools import find_packages, setup


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake_thorlabs',
    version='0.0.1.dev1',    
    description='Thorlabs plugin for Intake',
    url='https://github.com/scottcanoe/intake_thorlabs',
    maintainer='Scott Knudstrup',
    maintainer_email='scottknudstrup@gmail.com@gmail.com',
    license='GNU',
    py_modules=['intake_thorlabs'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': [
            'thorimagemetadata = intake_thorlabs.thorimage:ThorImageMetadataSource',
            'thorimagearray = intake_thorlabs.thorimage:ThorImageArraySource',
            'thorsync = intake_thorlabs.thorsync:ThorSyncSource',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    include_package_data=True,
    install_requires=requires,
    extras_require={},
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    zip_safe=False,
)
