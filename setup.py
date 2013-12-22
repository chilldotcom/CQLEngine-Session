from setuptools import setup, find_packages

setup(
    name='CQLEngine-Session',
    version='0.1',
    description="Session with identity map for cqlengine",
    long_description="Session with identity map for cqlengine",
    classifiers = [
        "Environment :: Web Environment",
        "Environment :: Plugins",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords='cassandra,cql,orm',
    install_requires = ['cqlengine>=0.10'],
    py_modules=[
        'cqlengine_session',
    ],
    author='Michael Cyrulnik',
    author_email='michael@chill.com',
    url='https://github.com/chilldotcom/CQLEngine-Session',
    license='BSD'#,
    #packages=find_packages(),
    #include_package_data=True,
)