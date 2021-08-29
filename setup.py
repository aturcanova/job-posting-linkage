from setuptools import setup, find_packages

setup(name='linkage',
      version='0.0.1',
      description='Job Posting Linkage',
      author='Andrea Turcanova',
      author_email='andrea.turcanova@tum.de',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      install_requires=[],
      python_requires='>=3.6',
      include_package_data=True,
      zip_safe=False)
