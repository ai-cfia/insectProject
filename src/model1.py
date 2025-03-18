# Databricks notebook source
import os
import torch
import torch.nn as nn
from torchvision import models


def getModel1(address):
    device = torch.device('cpu')
    
    deepNet = models.densenet121(pretrained=False)
    num_ftrs = deepNet.classifier.in_features
    deepNet.classifier = nn.Linear(num_ftrs, 2)
    input_size = 224
    
    model = deepNet
    checkpoint = torch.load(address,map_location=device)
    
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    return model 

# COMMAND ----------

