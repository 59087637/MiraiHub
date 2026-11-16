"""平台模型映射：`{下游名: 上游 model_id}` 的读写接口（控制台「模型映射」页）。

为什么单独一个 router：这张表影响**转发用的模型名**和**对外脱敏**，
排障时要能一眼看到「现在到底把哪个名字发去了哪个上游」，所以给只读视图
`/redactions` —— 它返回的正是中间件实际在用的替换表。
"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from admin import model_map
from admin.db import get_db
from admin.security import require_admin

router = APIRouter(prefix="/api/model-map", tags=["model-map"])


class ModelMapIn(BaseModel):
    hide_upstream: bool = True
    platforms: dict[str, dict[str, str]] = Field(default_factory=dict)


@router.get("")
def get_model_map(_: bool = Depends(require_admin),
                  db: Session = Depends(get_db)):
    data = model_map.load(db)
    return {"ok": True, **data}


@router.put("")
def put_model_map(payload: ModelMapIn,
                  _: bool = Depends(require_admin),
                  db: Session = Depends(get_db)):
    saved = model_map.save(db, payload.model_dump())
    return {"ok": True, **saved}


@router.get("/redactions")
def get_redactions(_: bool = Depends(require_admin),
                   db: Session = Depends(get_db)):
    """当前生效的脱敏表：`{上游 model_id: 下游可见名}`。"""
    data = model_map.load(db)
    pairs = model_map.redaction_pairs(db)
    return {"ok": True, "hide_upstream": data.get("hide_upstream", True),
            "pairs": pairs, "count": len(pairs)}
