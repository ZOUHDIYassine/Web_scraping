from fastapi import APIRouter, HTTPException
from models.todos import Todo
from config.database import collection_name
from schema.schemas import list_serial, individual_serial
from bson import ObjectId


router=APIRouter()

@router.get("/")
async def get_todos():
    todos=list_serial(collection_name.find())
    return todos

@router.get("/by-title/")
async def get_todo_by_title(title: str):
    todo = collection_name.find_one({"title": title})
    if not todo:
        raise HTTPException(status_code=404, detail="Todo not found")
    return individual_serial(todo)
@router.get("/top-topics/")
async def get_top_topics():
    try:
        pipeline = [
            {"$unwind": "$topics"},  # Unwind the Topics array
            {"$group": {"_id": "$topics", "count": {"$sum": 1}}},  # Group by topic and count occurrences
            {"$sort": {"count": -1}},  # Sort by count in descending order
            {"$limit": 5}  # Limit to top 5 topics
        ]
        top_topics = list(collection_name.aggregate(pipeline))
        return [{"topic": topic["_id"], "count": topic["count"]} for topic in top_topics]
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")
