from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.dashboard.service import get_dashboard_data

router = APIRouter()


@router.get("/")
def index():

    return FileResponse("app/dashboard/index.html")


@router.get("/api/dashboard")
def dashboard():

    return get_dashboard_data()
