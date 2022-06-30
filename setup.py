import setuptools
 
setuptools.setup(
    name="twitter",
    version="0.1.0",
    author="Steven Hurwitt",
    author_email="stevenhurwitt@gmail.com",
    description="TWITTER API",
    long_description="kafka producer and spark streaming application for twitter structured streaming.",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)
