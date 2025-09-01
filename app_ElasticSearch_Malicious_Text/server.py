from manager import Manager
from fastapi import FastAPI,HTTPException
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

print("is connected")
manage_system:Manager

@app.on_event("startup")
def manage_the_all_system():
    global manage_system
    manage_system = Manager()
    manage_system.insert_from_the_csv()
    manage_system.update_new_fields()
    manage_system.delete_not_relevant()

@app.get("/read_from_elastic")
def read_from_mongo_anti():
    try:
        all_documents = manage_system.crud.search_by_query()
        print(all_documents)
        return {"all_documents":all_documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8005)



