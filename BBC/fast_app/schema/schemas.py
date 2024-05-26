def individual_serial(todo) -> dict:
    return{
        "id": str(todo["_id"]),
        "link":str(todo["link"]),
        "subtitle":str(todo["subtitle"]),
        "Title":str(todo["title"]),
        "Topics":list(todo["topics"]),
        "Time":str(todo["time"]),
        "Images":list(todo["images"]),
        "Author":str(todo["author"]),
        "Text":list(todo["text"])
    }
def list_serial(todos) -> list:
    return[individual_serial(todo) for todo in todos]