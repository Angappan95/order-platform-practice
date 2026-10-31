from uuid import uuid4
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

class DomainError(Exception):
    def __init__(self, code, message, status_code=409):
        self.code, self.message, self.status_code = code, message, status_code


def install_error_handlers(app:FastAPI):

    @app.exception_handler(DomainError)
    async def domain_error(_, exc):
        trace_id = str(uuid4())
        return JSONResponse(
            status_code = exc.status_code,
            content = {
                "code": exc.code, "message": exc.message, "trace_id": trace_id
            }
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(__, exc):
        trace_id = str(uuid4())
        return JSONResponse(
            status_code=422,
            content={
                "code": "VALIDATION_ERROR",
                "message": "Request validation failed",
                "trace_id": trace_id,
                "details": [
                    {"field": ".".join(map(str, e["loc"])), "message": e["msg"]}
                    for e in exc.errors()
                ]
            }
        )