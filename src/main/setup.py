import setuptools
 
setuptools.setup(
    name="twitter",
    version="0.1.0",
    author="steven hurwitt",
    author_email="stevenhurwitt@gmail.com",
    description="twitter api dynamodb",
    long_description="twitter api scrape to dynamodb.",
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)