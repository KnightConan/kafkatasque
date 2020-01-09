from setuptools import setup

try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    from pip.req import parse_requirements

REQUIRED = [str(r.req) for r in parse_requirements('requirements.txt',
                                                   session=False)]
TEST_REQUIRED = [str(r.req) for r in parse_requirements('test-requirements.txt',
                                                        session=False)]

# Load README as description of package
with open('README.md') as readme_file:
    long_description = readme_file.read()

# Get current version
version = __import__('kafkatasque').__version__

setup(
    name='kafkatasque',
    version=version,
    author='Zhiwei Zhang',
    author_email='zhiwei2017@gmail.com',
    description='Kafka-based Task Queue',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://gitlab.gda.allianz/tasker/tasker',
    packages=["kafkatasque"],
    zip_safe=False,
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation",
        "Topic :: Utilities",
        "Natural Language :: English",
        "Intended Audience :: Developers",
    ],
    install_requires=REQUIRED,
    tests_require=TEST_REQUIRED,
    test_suite="tests",
)
