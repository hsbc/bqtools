import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='bqtools-json',
     version='0.4.1',
     scripts=[] ,
     author="Mike Moore",
     author_email="z_z_zebra@yahoo.com",
     description="A Big Query json utility package",
     long_description_content_type='text/markdown',
     long_description=long_description,
     url="https://github.com/Mikemoore63/bqtools",
     packages=setuptools.find_packages(),
     test_suite='nose.collector',
     tests_require=['nose'],
     install_requires=[
         "jinja2",
         "google-cloud",
         "datetime",
         "google-cloud-bigquery",
         "logging",
         "pprint"
      ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],

 )
