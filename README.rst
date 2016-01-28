Owl Mqtt Server
==================

.. image:: owl.png

Installation
-----------------

.. code-block:: bash

    sudo apt-get install mariadb-server mariadb-client
    sudo apt-get install python-dev
    sudo apt-get install libmysqlclient-dev
    sudo pip install mysql-python

`Owl <https://github.com/codemeow5/owl>`_ is a MQTT broker based on `Tornado <http://www.tornadoweb.org>`_,
a uncomplete implementation of MQTT protocol v3.1.
Future releases of Owl will support multi process and distributed.

Example
------------

Here is a simple example using Owl:

.. code-block:: python

    import tornado.ioloop
    import tornado.web
    from tornado.mqttserver import MqttServer

    if __name__ == "__main__":
        server = MqttServer()
        # Will be supported in the future
        # server.bind(8888)
        # server.start(0)
        server.listen(8888)
        tornado.ioloop.IOLoop.current().start()

This is an example of a single process, multi-process will be supported in the future.

Documentation
-------------

Documentation and links to additional resources are available at
http://www.tornadoweb.org
