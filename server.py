import socket
import string
import struct
import threading
import subprocess as sp
import queue
import cv2
import numpy as np
from ctypes import *
""" This class defines a C-like struct """


class VideoInfo(Structure):
    _fields_ = [("fps", c_uint32), ("height", c_uint32), ("width", c_uint32)]


class DataSize(Structure):
    _fields_ = [("size", c_uint32)]


rtspUrl = "rtsp://0.0.0.0:6006/live/test"
rtmpUrl = "rtmp://0.0.0.0:6006/"
resourceIndex = 0


def recv_size(sock, count):
    buf = b''
    while count:
        newbuf = sock.recv(count)
        if not newbuf:
            return None
        buf += newbuf
        count -= len(newbuf)
    return buf


class Push_To_Server(object):

    def __init__(self, url, conn, bufferSize=30):
        # rstpUrl为推流服务器地址
        self.err = None
        self.conn = conn
        self.url = url
        self.fps = 10
        self.h = 720
        self.w = 1280
        # self.buffer_mutex = threading.Lock()
        self.buffer = queue.Queue()
        self.command = [
            'ffmpeg', '-y', '-f', 'rawvideo', '-vcodec', 'rawvideo',
            '-pix_fmt', 'bgr24', '-s', "{}x{}".format(self.w, self.h), '-r',
            str(self.fps), '-i', '-', '-c:v', 'libx264', '-pix_fmt', 'yuv420p',
            '-rtsp_transport', 'tcp', '-preset', 'ultrafast', '-f', 'rtsp',
            self.url
        ]

    def frame_pulls(self):
        while not self.err:
            bytesData = self.conn.recv(4)
            datasize = DataSize.from_buffer_copy(bytesData)
            bytesData = recv_size(self.conn, datasize.size)
            print('Image recieved successfully! size={:d}'.format(
                datasize.size))
            nparr = np.frombuffer(bytesData, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            self.buffer.put(frame)

    def frame_push(self):
        fourcc = cv2.VideoWriter_fourcc(*'XVID')
        while True:
            if len(self.command) > 0:
                # 管道配置
                p = sp.Popen(self.command, stdin=sp.PIPE)
                break
        i = 0
        while not self.err:
            if self.buffer.empty() != True:
                i += 1
                frame = self.buffer.get()
                # cv2.imwrite('./imgs/img{}.jpg'.format(i), frame)
                p.stdin.write(frame.tobytes())
                print(f"Push frame{i} successfully")
        p.kill()
        print(f"error!{self.err}")

    def run(self):
        '''read config'''
        bytesData = self.conn.recv(12)
        videoinfo = VideoInfo.from_buffer_copy(bytesData)
        print("Received fps={:d}, height={:d}, width={:d}".format(
            videoinfo.fps, videoinfo.height, videoinfo.width))
        self.fps, self.h, self.w = videoinfo.fps, videoinfo.height, videoinfo.width
        self.command = [
            'ffmpeg', 
            '-y', '-an',
            '-f', 'rawvideo', 
            '-vcodec', 'rawvideo',
            '-pix_fmt', 'bgr24',
            '-s', "{}x{}".format(self.w, self.h), 
            '-r', str(self.fps), 
            '-i', '-', 
            '-c:v', 'libx264', 
            '-preset', 'ultrafast', 
            '-pix_fmt', 'yuv420p',
            '-rtsp_transport', 'tcp', 
            '-f', 'rtsp',
            # '-flvflags', 'no_duration_filesize',
            # '-f', 'flv',
            self.url
        ]
        threads = [
            threading.Thread(target=Push_To_Server.frame_pulls, args=(self, )),
            threading.Thread(target=Push_To_Server.frame_push, args=(self, ))
        ]
        for thread in threads:
            thread.start()


# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
server_address = ('0.0.0.0', 6005)
print('starting up on {} port {}'.format(*server_address))
sock.bind(server_address)

# Listen for incoming connections, max connection amount is 10
sock.listen(10)

while True:
    # Wait for a request
    print('waiting for a connection')
    conn, addr = sock.accept()
    print('connected from ', addr)
    resourceIndex += 1
    push_to_server = Push_To_Server(url=rtspUrl,
                                    conn=conn)
    push_to_server.run()

