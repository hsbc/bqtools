import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='bqtools-json',
     version='0.4.21',
     scripts=[] ,
     author="Mike Moore",
     author_email="z_z_zebra@yahoo.com",
     description="A Big Query json utility package",
     long_description_content_type='text/markdown',
     long_description=long_description,
     url="https://github.com/Mikemoore63/bqtools",
     packages=setuptools.find_packages(),
     test_suite='nose.collector',
     use_2to3=True,
     tests_require=['nose'],
     include_package_data=True,
     install_requires=[
         "jinja2",
         "google-cloud",
         "datetime",
         "google-cloud-bigquery",
         "pprint",
         "deepdiff>=3.3.0"
      ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],

 )
