Owl Mqtt Server
==================

.. image:: logo.png

Installation
-----------------

.. code-block:: bash

    sudo ./dependenceInstall.sh

`Owl <https://github.com/codemeow5/owl>`_ is a MQTT broker based on `Tornado <http://www.tornadoweb.org>`_,
a uncomplete implementation of MQTT protocol v3.1.
Future releases of Owl will support multi process and distributed.

Example
------------

Here is a simple example using Owl:

.. code-block:: python

    from tornado.ioloop import IOLoop
    from tornado.mqttserver import MqttServer

    if __name__ == "__main__":
        server = MqttServer()
        # Will be supported in the future
        # server.bind(8888)
        # server.start(0)
        server.listen(8888)
        IOLoop.current().start()

This is an example of a single process, multi-process will be supported in the future.

Test
------------

Owl is tested against Eclipse Paho's `MQTT Conformance/Interoperability Testing <http://www.eclipse.org/paho/clients/testing>`_.

.. code-block:: bash

    hostname localhost port 1883
    clean up starting
    clean up finished
    Basic test starting
    Basic test succeeded
    Retained message test starting
    Retained message test succeeded
    This server is not queueing QoS 0 messages for offline clients
    Offline message queueing test succeeded
    Will message test succeeded
    Overlapping subscriptions test starting
    This server is publishing one message per each matching overlapping subscription.
    Overlapping subscriptions test succeeded
    Keepalive test starting
    Keepalive test succeeded
    Redelivery on reconnect test starting
    Redelivery on reconnect test succeeded
    test suite succeeded

Documentation
-------------

Documentation and links to additional resources are available at
http://www.tornadoweb.org
