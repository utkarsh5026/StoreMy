Welcome to StoreMy's documentation!
====================================

StoreMy is a Python database project that provides storage, concurrency, and query functionality.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules
   api

Getting Started
===============

Installation
------------

1. Make sure you have Poetry installed
2. Clone this repository
3. Install dependencies:

.. code-block:: bash

   poetry install

Usage
-----

Run the application:

.. code-block:: bash

   poetry run app

Or activate the virtual environment and run:

.. code-block:: bash

   poetry shell
   python -m app.main

Development
===========

Run tests:

.. code-block:: bash

   poetry run pytest

Format code:

.. code-block:: bash

   poetry run black .

Lint code:

.. code-block:: bash

   poetry run flake8 .

Type check:

.. code-block:: bash

   poetry run mypy .

Generate documentation:

.. code-block:: bash

   poetry run docs

API Reference
=============

.. toctree::
   :maxdepth: 4

   app

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search` 