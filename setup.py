import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyLB",
    version="0.0.1",
    author="Anvith J Shetty",
    author_email="ashetty.undef@gmail.com",
    description="a server that distributes messages to the workers based on the queue load of the workers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ashetty-undef/py-load_balancer.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: OS Independent",
    ],
)
