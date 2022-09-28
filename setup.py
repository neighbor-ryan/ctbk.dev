from setuptools import setup, find_packages

setup(
    name="ctbk",
    use_scm_version=True,
    packages=find_packages(),
    install_requires=open('requirements.txt', 'r').read(),
)
