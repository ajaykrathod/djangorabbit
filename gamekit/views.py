# set async_mode to 'threading', 'eventlet', 'gevent' or 'gevent_uwsgi' to
# force a mode else, the best mode is selected automatically from what's
# installed
async_mode = None

from base64 import decode
import os
from django.dispatch import receiver
from django.shortcuts import render
import socketio
import pika
from marshmallow import Schema, fields


import json


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)



basedir = os.path.dirname(os.path.realpath(__file__))
sio = socketio.Server(async_mode=async_mode)
thread = None

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)

channel = connection.channel()
users = set()
messages = list()

def index(request):
    global thread
    if thread is None:
        thread = sio.start_background_task(background_thread)
    return render(request,'index.html')


def background_thread():
    """Example of how to send server generated events to clients."""
    count = 0
    while True:
        sio.sleep(10)
        count += 1
        sio.emit('my_response', {'data': 'Server generated event'},
                 namespace='/test')

def callback(response,queue_name,sid,message):
        if response:
            channel = connection.channel()
            q = channel.queue_declare(queue=queue_name, durable=True)
            q_len = q.method.message_count
            if q_len > 0:
                for method_frame, properties, body in channel.consume(queue_name):

                    # Display the message parts

                    # Acknowledge the message
                    channel.basic_ack(method_frame.delivery_tag)
                    # Escape out of the loop after 10 messages
                    if method_frame.delivery_tag == 1:
                        channel.cancel()
                        break

                    if method_frame.delivery_tag >= q_len:
                        channel.stop_consuming()
                        channel.cancel()
                        break
            # channel.queue_declare(queue=queue_name, durable=True)
            # channel.basic_qos(prefetch_count=1)
            # channel.basic_consume(queue=queue_name,on_message_callback=lambda ch, method, properties, body: room_event_callback(ch,method,properties,body,sid,message))
            # channel.start_consuming()

def message_callback(ch, method, properties, body,sid,message):
    recieved = body.decode()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.stop_consuming()
    # sio.emit('my_response', {'data': recieved,'sender':message['sender'],'receiver':message['receiver']},
    #          room=message['room'])
    pass
     
    

def room_event_callback(ch, method, properties, body,sid,message):
    recieved = body.decode()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.stop_consuming()
    # sio.emit('my_response', {'data': recieved,'sender':message['sender'],'receiver':message['receiver']},
    #          room=message['room'])
    pass


@sio.event
def connectionEstablised(sid, message):
    users.add(message['data'])
    dictionary = {}
    for user in users:
        dictionary[user] = user
    sio.emit('connection_response', {'data': dictionary})


# @sio.event
# def my_broadcast_event(sid, message):
#     sio.emit('my_response', {'data': message['data']})


@sio.event
def join(sid, message):
    try:
        channel = connection.channel()
        queue_name = message['receiver'] + message['sender']
        q = channel.queue_declare(queue=queue_name, durable=True)
        q_len = q.method.message_count
        channel.basic_qos(prefetch_count=1)
        if q_len > 0:
            for method_frame, properties, body in channel.consume(queue_name):
                # Display the message parts
                messages.append(body.decode())
                # Acknowledge the message
                channel.basic_ack(method_frame.delivery_tag)
                if method_frame.delivery_tag >= q_len:
                    channel.stop_consuming()
                    channel.cancel()
                    break

    except BaseException as exception:
        print(exception)
    
    # channel.basic_consume(queue=queue_name,on_message_callback=lambda ch, method, properties, body: message_callback(ch,method,properties,body,sid,message))
    # channel.start_consuming()
    
    sio.enter_room(sid,message['room'])
    if len(messages) > 0:
        print(messages)
        for msg in messages:
            loaded_str = json.loads(msg)
            # print(type(loaded_str))
            sio.emit('my_response', {'data': loaded_str['data'],'sender':loaded_str['sender'],'receiver':loaded_str['receiver'],'date':loaded_str['data']},
                room=message['room'])


@sio.event
def leave(sid, message):
    sio.leave_room(sid, message['room'])
    sio.emit('my_response', {'data': 'Left room: ' + message['room']},
             room=message['room'])


@sio.event
def close_room(sid, message):
    sio.emit('my_response',
             {'data': 'Room ' + message['room'] + ' is closing.'},
             room=message['room'])
    sio.close_room(message['room'])

@sio.event
def my_room_event(sid, message):
    queue_name = message['sender'] + message['receiver']
    try:
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode = pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
    except pika.exceptions.ConnectionClosedByBroker:
        print("Closed Error",pika.exceptions.ConnectionClosedByBroker)
    # Do not recover on channel errors
    except pika.exceptions.AMQPChannelError:
        print("AMQPChannel Error",pika.exceptions.AMQPChannelError)
    # Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError as connectionError:
        print("Connection Error",connectionError)
    sio.emit('message', {'data': message['data'],'sender':message['sender']}, room=message['room'],callback=lambda response:callback(response,queue_name=queue_name,sid=sid,message=message))


@sio.event
def disconnect_request():
    sio.disconnect()


@sio.event
def connect(sid, environ):
    pass
    # sio.emit('my_response', {'data': 'Connected', 'count': 0}, room=sid)


@sio.event
def disconnect():
    print('Client disconnected')