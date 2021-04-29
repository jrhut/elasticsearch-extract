
import setuptools

setuptools.setup(
    name="esextract",                     # This is the name of the package
    version="0.0.1",                        # The initial release version
    author="Jamie Hutchings",                     # Full name of the author
    description="Makes extracting queries from elasticseach a little bit easier.",
    packages=setuptools.find_packages(),    # List of all python modules to be installed
    python_requires='>=3.8',                # Minimum version requirement of the package
    license='MIT',
    py_modules=["esextract"],             # Name of the python package
    package_dir={'':'esextract/src'},     # Directory of the source code of the package
    install_requires=['elasticsearch',
        'pandas',
        'argparse',
        'pyarrow']                          # Install other dependencies if any
)
