from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    requirements = f.read().splitlines()

setup(
    name='USGeoCoder',
    version='0.1',
    author='Clay Gendron',
    author_email='chg@claygendron.io',
    description='USGeoCoder is an easy and free-to-use package for geocoding US addresses with the US Census Geocoder API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ClayGendron/usgeocoder',
    packages=find_packages(),
    install_requires=requirements
)
