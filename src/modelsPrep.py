# Databricks notebook source
# MAGIC %run ./model1

# COMMAND ----------

import torch
import io
import torchvision.transforms as transforms
from PIL import Image
import urllib.request

def getModels():
    models=[] 
    nonInvasive1= {"name": "Monochamus scutellatus", 
                   "id": "82043"}
    
    address_model1='' # TODO
    model1=getModel1(address_model1)
    m1 = {"invasive": ["Asian Long-horned Beetle","Citrus Longhorn Beetle"], # use common name because the way they are flagging these species specifically are by using the upper taxa so the scientific name would be that of the upper taxa and not of the invasive subspecies inside of it (i.e., not the ones we are targetting)
          #{ "invasive": ["Anoplophora glabripennis","Anoplophora chinensis"],
          "nonInvasive": [nonInvasive1]
          ,"model": model1}
    # Anoplophora glabripennis: Asian Longhorned Beetle
    # Anoplophora chinensis   : Citrus Longhorned Beetle
    # Monochamus scutellatus  : White Spotted Sawyer Beetle 
    models=models+[m1]
    return models 

def predict(image_sets,model,initial):
    predictions=[] 
    i=0
    for observation_image_set in image_sets: 
        prediction_i=initial# initialized to zero 
        # observation could have no images 
        print(i)
        for image_url in observation_image_set: 
            if "copyright" in image_url: 
                currentPrediction='REMOVED-COPYRIGHT'
            else:
            #load image 
            #image_url_large=createlinktofullimageVer2(image_url) no need already transformed 
            #tobefixed here 
                save_image(image_url)
                with open('curr.jpg', 'rb') as image:
                    f = image.read()
                    currentPrediction=get_prediction(f,model)
                            #get prediction
            
            if currentPrediction!='non_invasive': # doesn't change if  
                prediction_i=currentPrediction
            #if any is invasive flag observation 
        predictions.append(prediction_i)
        i=i+1
    return predictions 






def save_image(url):
    with urllib.request.urlopen(url) as i:
        byteImg = io.BytesIO(i.read())
        image = Image.open(byteImg)
        if image.mode in ("RGBA", "P"): 
            image = image.convert("RGB")
        image.save('curr.jpg')


















def transform_image(image_bytes):
    my_transforms = transforms.Compose([transforms.Resize(255),
                                        transforms.CenterCrop(224),
                                        transforms.ToTensor(),
                                        transforms.Normalize(
                                            [0.485, 0.456, 0.406],
                                            [0.229, 0.224, 0.225])])
    image = Image.open(io.BytesIO(image_bytes))
    return my_transforms(image).unsqueeze(0)
def get_prediction(image_bytes,model):
    imagenet_class_index = {'invasive': 0, 'non_invasive': 1}
    
    imagenet_class_index = {v: k for k, v in imagenet_class_index.items()}
    
    tensor = transform_image(image_bytes=image_bytes)
    outputs = model.forward(tensor)
    _, y_hat = outputs.max(1)
    predicted_idx = (y_hat.item())
    return imagenet_class_index[predicted_idx]

def get_prediction_probability(image_bytes,model):
    tensor = transform_image(image_bytes=image_bytes)
    outputs = model.forward(tensor)
    m = torch.nn.functional.softmax(outputs)

    _, y_hat = m.max(1)
    predicted_idx = (y_hat.item())
    probability = _

    return (probability)

# COMMAND ----------

