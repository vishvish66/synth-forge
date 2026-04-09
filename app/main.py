from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette import status

from app.api.routes.generate import router as generate_router
from app.core.config import get_settings


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(generate_router, prefix="/api/v1")


@app.get("/", tags=["root"])
def root() -> dict:
    return {
        "message": "SynthForge backend is running",
        "health": "/health",
        "docs": "/docs",
        "generate_endpoint": "/api/v1/generate",
    }


@app.get("/health", tags=["health"])
def health_check() -> dict:
    return {"status": "ok", "service": settings.app_name, "version": settings.app_version}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors(), "message": "Invalid request payload"},
    )
