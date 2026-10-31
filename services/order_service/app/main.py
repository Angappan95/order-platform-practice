from contextlib import asynccontextmanager
from fastapi import FastAPI
from .config import get_settings

from .api.errors import install_error_handlers
from .api.orders import router as orders_router



@asynccontextmanager
async def lifespan(app):
    settings = get_settings()
    app.state.settings = settings
    yield

def create_app():
    app = FastAPI(title = "Order Service", version = "0.1.0", lifespan=lifespan)

    install_error_handlers(app)
    app.include_router(orders_router)

    @app.get("/livez", include_in_schema=False)
    async def livez():
        return {"status": "alive"}

    @app.get("/readyz", include_in_schema=True)
    async def readyz():
        return {"status": "ready"}

    return app

app = create_app()
