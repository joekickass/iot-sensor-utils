#!/usr/bin/env python

import os
import argparse
import time
import threading
from Queue import Queue
import mosquitto

queue = Queue(10)

def main(host, sensors):
    print "#######################"
    print "Temperature poller v0.1"
    print "#######################"

    print "Using sensors:"
    pollers = []
    for sensor in sensors:
        print "    {sensor}".format(sensor=sensor)
        p = PollerThread(sensor)
        p.start()
        pollers.append(p)

    publisher = PublisherThread(host)
    publisher.start()    

    try:
        raw_input("Press key to exit")
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        for poller in pollers:
            poller.stop()
        publisher.stop()
        # Make sure publisher is not stuck waiting for data
        queue.put(None)


class StoppableThread(threading.Thread):

    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def is_stopped(self):
        return self._stop.isSet()


class PollerThread(StoppableThread):

    def __init__(self, sensor):
        super(PollerThread, self).__init__()
        self.sensor = sensor
        self.id = os.path.dirname(sensor)

    def run(self):
        global queue
        while not self.is_stopped():
            temp = self.get_temp()
            queue.put((self.id, temp))
            time.sleep(1)

    def get_temp(self):
        temp = -1
        with open(self.sensor, 'rb') as s:
            temp = s.read()
        return temp

class PublisherThread(StoppableThread):

    def __init__(self, host="test.mosquitto.org"):
        self.mqttc = mosquitto.Mosquitto("python_pub")
        self.mqttc.will_set("/event/dropped", "Sorry, I seem to have died.")
        self.mqttc.connect(host, 1883, 60, True)

    def run(self):
        global queue
        while not self.is_stopped():
            ret = queue.get()
            if ret:
                (id, temp) = ret
                queue.task_done()
                mqttc.publish("iot_lab/temp", "{id}:{temp}".format(id=id, temp=temp))  


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('host')
    parser.add_argument('-s', '--sensors', default=[], nargs='+', help='path(s) to sensors, separated by space')
    args = parser.parse_args()
    main(args.host, args.sensors)
