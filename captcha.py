import cv2
import numpy as np
import random
import io
import base64
import json
import pickle
import  string

def im2json(im):
    """Convert a Numpy array to JSON string"""
    imdata = pickle.dumps(im)
    jstr = json.dumps({"image": base64.b64encode(imdata).decode('ascii')})
    return jstr

def rotateImage(image, angle):
    row,col = 100,250
    center=tuple(np.array([row,col])/2)
    rot_mat = cv2.getRotationMatrix2D(center,angle,1.0)
    new_image = cv2.warpAffine(image, rot_mat, (col,row))
    return new_image

def captchaGenerate():
    font = cv2.FONT_HERSHEY_COMPLEX
    captcha = np.zeros((50,250,3), np.uint8)
    captcha[:] = (random.randint(235,255),random.randint(245,255),random.randint(245,255))
    font= cv2.FONT_HERSHEY_SIMPLEX
    texcode = ''
    listCharector =  string.digits+string.ascii_lowercase+string.digits
    for i in range(1,5):
        bottomLeftCornerOfText = (random.randint(35,45)*i,35+(random.randint(-8,8)))
        fontScale= random.randint(7,15)/10
        fontColor= (random.randint(0,180),random.randint(0,180),random.randint(0,180))
        thickness= random.randint(1,2)
        lineType= 1
        text = str(listCharector[random.randint(0,len(listCharector)-1)])
        texcode = texcode+(text)
        cv2.putText(captcha,text,bottomLeftCornerOfText,font,fontScale,fontColor,thickness,lineType)
        if random.randint(0,2)>0:
            pt1 = (random.randint(0,250),random.randint(0,50))
            pt2 = (random.randint(0,250),random.randint(0,50))
            lineColor = (random.randint(0,150),random.randint(0,150),random.randint(0,150))
            cv2.line(captcha,pt1,pt2,lineColor,1)
    address = f'C:\\Users\\moeen\\Desktop\\project\\pishkar\\Front\\pishkar\\public\\captcha\\'+texcode+'.jpg'
    stringImg = base64.b64encode(cv2.imencode('.jpg', captcha)[1]).decode()
    return [texcode,stringImg]

