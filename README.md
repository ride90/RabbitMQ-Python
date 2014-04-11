RabbitMQ-Python
===============

Demos with python

    Prepare for work:
    1) Install RabbitMQ server:
    sudo apt-get install rabbitmq-server

    2) Install RabbitMQ management console:
    rabbitmq-plugins enable rabbitmq_management
    or
    sudo /usr/lib/rabbitmq/lib/rabbitmq_server-*.*.*/sbin/rabbitmq-plugins enable rabbitmq_management

    The following plugins have been enabled:
      mochiweb
      webmachine
      rabbitmq_mochiweb
      amqp_client
      rabbitmq_management_agent
      rabbitmq_management
    Plugin configuration has changed. Restart RabbitMQ for changes to take effect.
    3) Restart server:
        service rabbitmq-server restart


    4) Web ui:
    Protocol amqp must listen 5672
    # netstat -nlp | grep 567

    Check web ui:
        http://IP.IP.IP.IP:55672 or other port
        user: guest
        pswd: guest

    Download rabbitmqadmin at:
        http://IP.IP.IP.IP:55672/cli/

    The HTTP API and its documentation are both located
        http://IP.IP.IP.IP:55672/api/ or other port
