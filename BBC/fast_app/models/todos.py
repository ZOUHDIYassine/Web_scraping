from pydantic import BaseModel

class Todo(BaseModel):
    link:str
    subtitle:str
    Title:str
    Topics:str
    Text:str
    Time:str
    Images:str
    Author:str
    
