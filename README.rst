========
Python Power Service
========

Package generated to interface with a trading system.

Package will return a random set of open trades for a given day, where each trade contains data for hours, trading volume, and an unique ID.

Note: The package will naively return series for a given date and not take into account for example if the date is in the future.
Further the numbers for the volume are random and do not have any relationship with previous or subsequent numbers as would normally be the case in real data.

Installation
============
Through a terminal navigate to the folder you have the powerservice folder and run

::

    pip install ./python-powerservice


Documentation
=============

The service will be part of the python environment and can be called in code
::

    from powerservice import trading

Example that will output some trades
::
    from powerservice import trading
    trades = trading.get_trades("29/06/2021")

    print(trades)
    

Running Trades Aggregation
::

    'src/powerservice/trades_aggregation.py' is the main class to generate Trades Aggregation for LocalTime.
    This Application reads trades_data/trades.csv (which is generated by trading.py, saved the output as csv)
    trades_aggregation takes 'trades.csv' and 'date format' as input arguments.

Note:
::

    trades_aggregation implementation used PySpark. The code is executed in Databricks environment.
    Please find the 'databricks/powerservice - Databricks Community Edition.html' in the project.

