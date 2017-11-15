import setuptools

setuptools.setup(
    name='warehouse-in-google-cloud',
    version='0.1.0',
    install_requires=['luigi'],
    packages=setuptools.find_packages(),
    include_package_data=True
)
