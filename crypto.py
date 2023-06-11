from cryptography.fernet import Fernet


key = 'KPms1b_Kibq5XR6M0d88rJTsjjgdlBFzbFN4irIxiHo='
f = Fernet(key)

def encrypt(msg):
    msg = str(msg).encode()
    msg = f.encrypt(msg)
    return msg

def decrypt(msg):
    try:
        msg = f.decrypt(msg)
        msg = msg.decode()
        return msg
    except:
        return str({'main':''})
