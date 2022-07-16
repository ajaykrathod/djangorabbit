"""
WSGI config for djangorabbit project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.0/howto/deployment/wsgi/
"""

from asyncio import events
import os

from django.contrib.staticfiles.handlers import StaticFilesHandler
from gamekit.views import sio
from django.core.wsgi import get_wsgi_application
import socketio
import eventlet
import eventlet.wsgi

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'djangorabbit.settings')
django_app = StaticFilesHandler(get_wsgi_application())
application = socketio.Middleware(sio, wsgi_app=django_app,socketio_path='socket.io')

eventlet.wsgi.server(eventlet.listen(('',8000)),application)