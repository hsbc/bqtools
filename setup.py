import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='bqtools',
     version='0.1',
     scripts=[] ,
     author="Mike Moore",
     author_email="z_z_zebra@yahoo.com",
     description="A Big Query json utility package",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/Mikemoore63/bqtools",
     packages=setuptools.find_packages(),
     install_requires=[
         "jinja2"
      ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],

 )