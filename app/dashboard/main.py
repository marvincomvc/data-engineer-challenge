import uvicorn
from fastapi import FastAPI

from app.dashboard.api import router

app = FastAPI(title="Settlement CFO Dashboard")


app.include_router(router)


if __name__ == "__main__":
    uvicorn.run(
        "app.dashboard.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
    )
