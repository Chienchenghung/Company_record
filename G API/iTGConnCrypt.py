# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 18:11:01 2021

@author: david_yctsai
@description: decrypt connection string with secret.key/pymssql.bin
"""

from cryptography.fernet import Fernet


class iTGConnCrypt:
    key = None
    binfile = None

    def generate_key(self):
        # Generates a key and save it into a file
        key = Fernet.generate_key()
        with open("secret.key", "w+") as key_file:
            key_file.write(key.decode('ascii'))
        return key

    def loadKey(self):
        # 載入key
        return open("secret.key", "rb").read()

    def loadEncodeConn(self):
        # 載入Conn
        return open(self.binfile, "rb").read()

    def __init__(self, binfile='pymssql.bin'):
        self.key = self.loadKey()
        self.binfile = binfile
    
    def new_encrypt(self, connStr):
        # 依key加密str
        # load the Key
        key = self.generate_key()
        # encode the message
        message = connStr.encode()
        # initialize the Fernet class
        f = Fernet(key)
        # encrypt the message
        with open(self.binfile, "w+") as conn_file:
            conn_file.write(f.encrypt(message).decode('ascii'))

    def encrypt(self, connStr):
        # 依key加密str
        # encode the message
        message = connStr.encode()
        # initialize the Fernet class
        f = Fernet(self.key)
        # encrypt the message
        with open(self.binfile, "w+") as conn_file:
            conn_file.write(f.encrypt(message).decode('ascii'))

    def decrypt(self, connStr):
        # 依key解密str
        f = Fernet(self.key)
        return f.decrypt(connStr)

    def getDecConn(self):
        self.encStr = self.loadEncodeConn()
        # 取得解密後的ConnectString
        conn = self.decrypt(self.encStr)
        return conn.decode("utf-8")

if __name__=="__main__":
    myBinfile = 'console_log.bin'
    crypt = iTGConnCrypt(myBinfile)
    # print(crypt.getDecConn())
    crypt.encrypt(myConn)
    print(crypt.getDecConn())
    
