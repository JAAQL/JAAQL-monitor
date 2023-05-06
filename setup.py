from setuptools import find_packages, setup
from monitor.version import VERSION

REQUIREMENTS = [i.strip().replace("==", "~=") for i in open("requirements.txt").readlines() if "pyinstaller" not in i]

setup(
    name='jaaql-monitor',
    packages=find_packages(include=['monitor.*', 'monitor']),
    version=VERSION,
    url='https://github.com/JAAQL/JAAQL-monitor',
    description='How to interact with jaaql',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Software Quality Measurement and Improvement bv',
    author_email="aaron.tasker@sqmi.nl",
    license='Mozilla Public License Version 2.0 with Commons Clause',
    install_requires=REQUIREMENTS,
    package_data={'': ['config/*.ini', 'scripts/*.sql', 'migrations/*.sql', 'scripts/*.html']},
)
