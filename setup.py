from setuptools import find_packages, setup

setup(
    name='server',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask',
        'sklearn',
        'pandas',
        'requests',
        'gunicorn',
        'sqlalchemy',
        'psycopg2-binary'
    ],
)
