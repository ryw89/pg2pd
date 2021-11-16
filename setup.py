import pkg_resources
import setuptools
from setuptools.command.build_py import build_py
from setuptools_rust import Binding, RustExtension


def install_ext_modules(install_rust_ext):
    rust_modules = []

    installed = {pkg.key for pkg in pkg_resources.working_set}
    if 'setuptools-rust' in installed and install_rust_ext:
        rust_modules = [RustExtension('__pg2pd_rust', binding=Binding.PyO3)]

    return rust_modules


def run_setup(rust_modules):
    setuptools.setup(
        name='pg2pd',
        version='0.0.1',
        author=author,
        author_email=author_email,
        description='A Postgres binary data to Pandas parser',
        long_description=long_description,
        long_description_content_type='text/markdown',
        packages=setuptools.find_packages(),
        classifiers=[
            'Programming Language :: Python :: 3',
            'Operating System :: OS Independent',
        ],
        install_requires=install_requires,
        python_requires='>=3.6',
        rust_extensions=rust_modules,
        cmdclass={
            'build_py': BuildPy,
        },
    )

    if not rust_modules:
        print('Warning: The Rust extension modules were not installed.')


class BuildPy(build_py):
    def run(self):
        # We need to be building our extension modules first, so we'll
        # alter the build_py class from setuptools
        self.run_command('build_ext')
        super(build_py, self).run()


with open('README.md', 'r') as f:
    long_description = f.read()

install_requires = ['pandas']

authors = {'Ryan Whittingham': 'ryanwhittingham89@gmail.com'}

author = ', '.join(list(authors.keys()))
author_email = ', '.join(list(authors.values()))

install_rust_ext = True
rust_modules = install_ext_modules(install_rust_ext)
run_setup(rust_modules)
