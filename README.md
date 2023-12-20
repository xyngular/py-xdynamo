![PythonSupport](https://img.shields.io/static/v1?label=python&message=%203.10|%203.11|%203.12&color=blue?style=flat-square&logo=python)
![PyPI version](https://badge.fury.io/py/xdynamo.svg?)

This is pre-release software, and needs to be refactored in a few ways and needs more  natural documentation with examples.  Right now the documentation is in the ([API Reference](https://xyngular.GitHub.io/py-xdynamo/latest/api/xdynamo/)) documentation section.  The natural overview/high-level docs have yet to be written

Here are a few other related libraries I wrote written that are in a much better state with good documentation, if you want to see other examples of my work:

- https://github.com/xyngular/py-xinject
    - A simple dependency injection library
- https://github.com/xyngular/py-xcon
    - Fast dynamic configuration retriever
    - Can create a flat list of config values based on various config sources and paths environment (for environmental differences and so on).
- https://github.com/xyngular/py-xsettings
    - Centralize settings for a project.
    - Can be used with xcon (see above).
    - Know immediately if a setting can't be found as soon as it's asked for vs letting the incorrect setting value cause a problem/crash it happen later on, and then having to back-track it. 

## Documentation

**[📄 Detailed Documentation](https://xyngular.github.io/py-xdynamo/latest/)** | **[🐍 PyPi](https://pypi.org/project/xdynamo/)**

## Getting Started

**warning "Alpha Software!"**

This is pre-release Alpha software, based on another code base and
the needed changes to make a final release version are not yet
completed. Everything is subject to change; and documentation needs
to be written.


```shell
poetry install xdynamo
```

or

```shell
pip install xdynamo
```
