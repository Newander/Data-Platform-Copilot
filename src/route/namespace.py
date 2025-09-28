from fastapi import APIRouter

namespace_router = APIRouter(prefix='/namespace')

@namespace_router.get('/')
def list_namespaces():
    return
